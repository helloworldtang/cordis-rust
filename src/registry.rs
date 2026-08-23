//! 插件注册表与 `Plugin` trait。
//!
//! 对应原版 `ctx.registry`（RegistryService）与 `Plugin` 命名空间。
//! 原版支持函数/类/`{ apply }` 对象三种插件形态；Rust 版统一收敛为一个
//! trait——**插件是满足 `Send + Sync + 'static` 的任意类型，其 `apply`
//! 向上下文贡献服务、事件监听与可逆副作用**。
//!
//! 注册表以**插件实例的身份**（`Arc` 指针）为键：同一个实例多次挂载共享
//! 一条运行时记录、各得一个 fiber；这与原版“以 callback 身份为键”一致。
//! 注意与原版的差异：两个同类型的不同实例在 Rust 版是两条运行时记录，
//! 因为 Rust 插件在构造期捕获自己的配置（见 README“与原版的差异”）。

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::context::Context;
use crate::error::{CordisError, Result};
use crate::events::BoxFuture;

/// 插件：向共享上下文贡献服务、类型化事件与可逆副作用的对象。
///
/// 生命周期由框架驱动：
///
/// 1. [`Context::plugin`](crate::Context::plugin) 挂载后，fiber 进入
///    `Pending`，等待 [`inject`](Plugin::inject) 声明的全部服务可用；
/// 2. 依赖就绪后进入 `Loading`，框架调用 [`apply`](Plugin::apply)；
/// 3. `apply` 成功则进入 `Active`（其间注册的服务、监听器对外可见），
///    失败则进入 `Failed`；
/// 4. 任一依赖被撤销时，fiber 自动卸载（逆序执行副作用）回到 `Pending`，
///    依赖恢复后重新加载。
///
/// # 配置约定
///
/// v1 不做配置热更新：插件在构造时接收自己的配置（builder/结构体字面量），
/// `apply` 只负责挂载。原版 `fiber.update(config)` 与配置校验 schema
/// 详见 README“与原版的差异”。
pub trait Plugin: Send + Sync + 'static {
    /// 展示名，用于日志与诊断。
    // 实现方（如 FnPlugin）需要返回借用 self 的动态名字，不能收紧为 'static
    #[expect(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "anonymous"
    }

    /// 声明服务依赖：只有这些服务全部就绪（且提供方处于 `Active`）时，
    /// 本插件才会加载。加载顺序通过依赖表达，而非手动编排启动序列。
    fn inject(&self) -> Vec<String> {
        Vec::new()
    }

    /// 挂载本体。期间通过 `ctx.provide / ctx.on / ctx.effect` 注册的贡献
    /// 都是可逆副作用，会在卸载时按逆序撤销。
    ///
    /// 返回 `Err` 表示插件启动失败，fiber 转入 `Failed` 并记录错误，
    /// 已注册的副作用会被撤销。
    fn apply(&self, ctx: Context) -> BoxFuture<'static, Result<()>>;
}

/// 闭包插件：对应原版“函数插件”与 `ctx.inject(deps, callback)` 速记。
///
/// # Examples
///
/// ```
/// use cordis::{fn_plugin, prelude::*};
///
/// // 等待 "database" 服务就绪后运行一段一次性逻辑
/// let p = fn_plugin("migrator", &["database"], |ctx: Context| async move {
///     // ...读取 database 服务并执行迁移...
///     Ok(())
/// });
/// ```
pub struct FnPlugin<F> {
    name: String,
    inject: Vec<String>,
    body: F,
}

impl<F> FnPlugin<F> {
    /// 以名字、依赖列表与插件体构造。
    pub fn new(name: impl Into<String>, inject: &[&str], body: F) -> Self {
        Self {
            name: name.into(),
            inject: inject.iter().map(|s| (*s).to_string()).collect(),
            body,
        }
    }
}

impl<F, Fut> Plugin for FnPlugin<F>
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn inject(&self) -> Vec<String> {
        self.inject.clone()
    }

    fn apply(&self, ctx: Context) -> BoxFuture<'static, Result<()>> {
        Box::pin((self.body)(ctx))
    }
}

/// 构造闭包插件（见 [`FnPlugin`]）。
pub fn fn_plugin<F, Fut>(name: impl Into<String>, inject: &[&str], body: F) -> FnPlugin<F>
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    FnPlugin::new(name, inject, body)
}

/// 校验插件形态并返回其运行时键（`Arc` 指针地址）。
///
/// Rust 的类型系统已排除了原版“既不是函数也没有 apply”的非法形态，
/// 这里保留入口只为给出统一的错误通道。
pub(crate) fn runtime_key(plugin: &Arc<dyn Plugin>) -> usize {
    Arc::as_ptr(plugin).cast::<()>() as usize
}

/// 一条插件运行时记录：展示名 + 其全部活跃 fiber。
///
/// 插件本体由各 fiber 强引用保活（记录存在 ⇒ fiber 存在 ⇒ 插件存活），
/// 因此这里无需再持有 `Arc<dyn Plugin>`；键只是指针地址，不解引用。
pub(crate) struct RuntimeRecord {
    /// 插件展示名（首挂载时复制）。
    pub name: String,
    /// 该插件的全部活跃 fiber。
    pub fibers: Vec<Arc<crate::fiber::FiberInner>>,
}

/// 插件注册表（AppInner 的组成部分，非独立服务）。
pub(crate) struct Registry {
    runtimes: HashMap<usize, RuntimeRecord>,
}

impl Registry {
    /// 创建空注册表。
    pub(crate) fn new() -> Self {
        Self {
            runtimes: HashMap::new(),
        }
    }

    /// 挂载一个插件实例：复用或新建运行时记录，登记 fiber。
    pub(crate) fn mount(&mut self, plugin: &Arc<dyn Plugin>, fiber: Arc<crate::fiber::FiberInner>) {
        let key = runtime_key(plugin);
        let record = self.runtimes.entry(key).or_insert_with(|| RuntimeRecord {
            name: plugin.name().to_string(),
            fibers: Vec::new(),
        });
        record.fibers.push(fiber);
    }

    /// 摘除一个 fiber；其所属运行时记录在最后一个 fiber 移除后一并删除
    /// （对应原版 `registry.delete` 的级联语义）。
    pub(crate) fn unmount(&mut self, plugin: Option<&Arc<dyn Plugin>>, fiber_uid: u64) {
        let Some(plugin) = plugin else { return };
        let key = runtime_key(plugin);
        if let Some(record) = self.runtimes.get_mut(&key) {
            record.fibers.retain(|fiber| fiber.uid() != Some(fiber_uid));
            if record.fibers.is_empty() {
                self.runtimes.remove(&key);
            }
        }
    }

    /// 注册表中的运行时记录数（对应原版 `registry.size`）。
    pub(crate) fn size(&self) -> usize {
        self.runtimes.len()
    }

    /// 各运行时记录的展示名（诊断）。
    pub(crate) fn runtime_names(&self) -> Vec<String> {
        self.runtimes
            .values()
            .map(|record| record.name.clone())
            .collect()
    }

    /// 遍历所有活跃 fiber 的克隆（供依赖变化通知）。
    pub(crate) fn all_fibers(&self) -> Vec<Arc<crate::fiber::FiberInner>> {
        self.runtimes
            .values()
            .flat_map(|record| record.fibers.iter().cloned())
            .collect()
    }
}

/// 将插件启动错误包装为带插件名的框架错误。
pub(crate) fn plugin_error(plugin: &str, source: CordisError) -> CordisError {
    CordisError::PluginFailed {
        plugin: plugin.to_string(),
        source: Box::new(source),
    }
}
