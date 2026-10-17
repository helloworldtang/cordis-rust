//! 类型化事件总线：`emit` / `waterfall` / `parallel` / `serial` 四种分发
//! 模式，另有拦截扩展点专用的同步瀑布形态（见模块文档末节）。
//!
//! 对应原版 `ctx.events`（EventsService）。事件的**分发模式是事件公开契约的
//! 一部分**：每个事件名自首次注册起绑定一种模式与一种负载类型，此后用任何
//! 其他模式或类型注册/分发都会得到 [`CordisError::EventModeMismatch`] /
//! [`CordisError::EventPayloadMismatch`]——这是原版“@mode 标签 + 目录交叉
//! 校验”在 Rust 里的运行时对应。
//!
//! 各模式语义（与原版对照）：
//!
//! | 模式 | 是否等待 | 顺序 | 返回值 |
//! |---|---|---|---|
//! | `emit` | 否（触发即忘） | 注册顺序 | 无 |
//! | `waterfall` | 是（async） | 注册顺序，先注册者在外层 | 链条最终值 |
//! | `parallel` | 是 | 并发扇出 | 无（聚合错误） |
//! | `serial` | 是 | 注册顺序 | 首个 `Some`（bail） |
//! | `waterfall(sync)` | 是（同步） | 注册顺序，先注册者在外层 | 链条最终值 |
//!
//! ## 同步瀑布（拦截层）
//!
//! 原版 `waterfall` 本身是同步的（`next()` 直调直返，无 await）——它挂在
//! Proxy get/set trap 里，物理上不可能异步。框架用这一形态实现服务读写的
//! 拦截扩展点：`internal/get` / `internal/set`（见 [`event`] 常量），
//! 对应 [`EventBus::waterfall_sync`] 与 [`crate::Context::on_waterfall_sync`]。
//!
//! **防死锁铁律**：同步瀑布的监听器执行绝不持任何锁——快照（records 锁）
//! 释放后才走链，链尾的解析/写入再取服务锁；监听器内嵌套 `get` 因此安全。
//!
//! ## 派发上下文的流向
//!
//! 所有分发方法的第一参数是**派发方上下文**：监听器收到的 `ctx` 是"事件的
//! 来源"，而不是注册时的上下文——与原版 dispatch 的 `thisArg` 一致。监听器
//! 若需要自己的上下文，应在注册时闭包捕获。瀑布续体 [`Next`] 同样携带
//! 派发方上下文，保证链条上每一层看到同一个来源。
//!
//! 监听器通过 [`crate::Context::on`] 等方法注册，自动挂到当前 fiber 的
//! 副作用列表上，fiber 卸载时撤销。

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::context::Context;
use crate::error::{CordisError, Result};
use crate::logger::Logger;
use crate::{ListenOptions, ROOT_SCOPE, ScopeKey};

/// 装箱后的 `Send` future（避免引入 futures 库的依赖）。
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 内置框架事件名（对应原版 `Events` 接口中的 `internal/*` 事件）。
pub mod event {
    /// 有插件 fiber 被创建（负载 [`crate::Fiber`]）。
    pub const PLUGIN: &str = "internal/plugin";
    /// fiber 生命周期状态变化（负载 [`crate::StatusEvent`]）。
    pub const STATUS: &str = "internal/status";
    /// 服务提供/撤销（负载 [`crate::ServiceEvent`]）。
    pub const SERVICE: &str = "internal/service";
    /// 服务读取拦截（同步瀑布，值 [`crate::ServiceGet`]）。
    ///
    /// 派发点：`ctx.get` / `ctx.expect`。链尾初值是框架的真实解析结果，
    /// 拦截者可改写（shadow）、补默认（fallback）或清空（否决）。框架
    /// 内部的依赖就绪检查**不经过**此瀑布（对应原版 `_checkImpl` 直查
    /// store），依赖图不受读拦截影响。
    pub const GET: &str = "internal/get";
    /// 服务写入拦截（同步瀑布，值 [`crate::ServiceSet`]）。
    ///
    /// 派发点：`ctx.provide`。拦截者可改写拟写入的服务名/值，或以 `Err`
    /// 否决整个写入（错误传播给 provide 调用方）。
    pub const SET: &str = "internal/set";
    /// 事件即将分发（负载 [`crate::DispatchEvent`]）。
    ///
    /// 非框架事件的每次分发前触发（`internal/*` 自身豁免，防递归），
    /// 携带分发模式与事件名——事件总线的统一观测点。原版还携带
    /// 参数列表；类型化总线无法承载任意参数，这是有意的简化。
    pub const DISPATCH: &str = "internal/dispatch";
}

/// `internal/dispatch` 事件的负载：一次公开分发的模式与事件名。
#[derive(Debug, Clone)]
pub struct DispatchEvent {
    /// 分发模式。
    pub mode: DispatchMode,
    /// 被分发的事件名。
    pub name: String,
}

/// 分发模式（见模块级文档的语义对照表）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DispatchMode {
    /// 触发即忘：不等待监听器，错误记入日志。
    Emit,
    /// 瀑布：监听器层层包装，必须调用 `next()` 委托，否则短路。
    Waterfall,
    /// 并发：所有监听器同时观察，等待全部完成。
    Parallel,
    /// 串行：按注册顺序执行，首个返回 `Some` 者短路。
    Serial,
}

impl DispatchMode {
    /// 模式名（用于 `EventModeMismatch` 诊断）。
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Emit => "emit",
            Self::Waterfall => "waterfall",
            Self::Parallel => "parallel",
            Self::Serial => "serial",
        }
    }
}

/// 观察型监听器（emit / parallel 共用）的类型擦除形态。
type ObserveErased = Arc<
    dyn Fn(Context, Arc<dyn Any + Send + Sync>) -> BoxFuture<'static, Result<()>> + Send + Sync,
>;
/// 串行型监听器的类型擦除形态：返回 `Some` 即 bail。
type SerialErased = Arc<
    dyn Fn(
            Context,
            Arc<dyn Any + Send + Sync>,
        ) -> BoxFuture<'static, Result<Option<Box<dyn Any + Send>>>>
        + Send
        + Sync,
>;
/// 瀑布型监听器的类型擦除形态：消费一个擦除的续体。
type WaterfallErased = Arc<
    dyn Fn(
            Context,
            Box<dyn Any + Send>,
            NextErased,
        ) -> BoxFuture<'static, Result<Box<dyn Any + Send>>>
        + Send
        + Sync,
>;
/// 瀑布续体的类型擦除形态：吃入一个值，吐出链条的最终值。
type NextErased = Box<
    dyn FnOnce(Context, Box<dyn Any + Send>) -> BoxFuture<'static, Result<Box<dyn Any + Send>>>
        + Send,
>;
/// 同步瀑布型监听器的类型擦除形态。
type SyncWaterfallErased = Arc<
    dyn Fn(Context, Box<dyn Any + Send>, SyncNextErased) -> Result<Box<dyn Any + Send>>
        + Send
        + Sync,
>;
/// 同步瀑布续体的类型擦除形态。
type SyncNextErased =
    Box<dyn FnOnce(Context, Box<dyn Any + Send>) -> Result<Box<dyn Any + Send>> + Send>;

/// 事件总线内部状态。
struct BusInner {
    /// 事件名 → 注册记录。
    records: Mutex<HashMap<String, EventRecord>>,
    /// 监听器 id 发生器。
    next_id: std::sync::atomic::AtomicU64,
    /// emit 模式下监听器失败时的报告出口。
    logger: Logger,
}

/// 单个事件的注册记录：监听形态契约 + 监听器列表。
struct EventRecord {
    contract: Contract,
    hooks: Vec<Hook>,
}

/// 一个事件的监听形态契约：允许的监听器种类与负载/返回类型。
///
/// emit 与 parallel 共享观察形态（Observe），waterfall 与 serial 各有
/// 专属形态——对应原版"事件的分发模式是公开契约"的约束。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Contract {
    /// 监听器形态。
    kind: ContractKind,
    /// 负载类型。
    payload: TypeId,
    /// serial 的 bail / waterfall 的链值类型（观察形态为 `None`）。
    ret: Option<TypeId>,
}

/// 监听器形态（emit/parallel 共用 Observe）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContractKind {
    /// 观察者：emit 与 parallel 都可以分发。
    Observe,
    /// 瀑布包装层。
    Waterfall,
    /// 串行决策者。
    Serial,
    /// 同步瀑布包装层（拦截扩展点专用）。
    SyncWaterfall,
}

impl Contract {
    /// 观察形态的契约。
    fn observe<T: Send + Sync + 'static>() -> Self {
        Self {
            kind: ContractKind::Observe,
            payload: TypeId::of::<T>(),
            ret: None,
        }
    }

    /// 串行形态的契约。
    fn serial<T: Send + Sync + 'static, R: Send + 'static>() -> Self {
        Self {
            kind: ContractKind::Serial,
            payload: TypeId::of::<T>(),
            ret: Some(TypeId::of::<R>()),
        }
    }

    /// 瀑布形态的契约。
    fn waterfall<T: Send + Sync + 'static>() -> Self {
        Self {
            kind: ContractKind::Waterfall,
            payload: TypeId::of::<T>(),
            ret: Some(TypeId::of::<T>()),
        }
    }

    /// 同步瀑布形态的契约。
    fn waterfall_sync<T: Send + Sync + 'static>() -> Self {
        Self {
            kind: ContractKind::SyncWaterfall,
            payload: TypeId::of::<T>(),
            ret: Some(TypeId::of::<T>()),
        }
    }
}

/// 一个已注册的监听器。
struct Hook {
    id: u64,
    /// 只触发一次：取快照（派发开始）时同步摘除。
    once: bool,
    /// 豁免作用域过滤（对应原版 `EventOptions.global`）。
    global: bool,
    /// 注册方上下文的隔离快照（服务名 → 作用域键，内层覆盖外层）。
    ///
    /// 作用域过滤的判定：监听器看到服务 `s` 的变更事件，当且仅当
    /// `scopes.get(s).unwrap_or(ROOT_SCOPE)` 与派发方声明的作用域键相等
    /// ——与原版 `ctx[isolate][name] === provider[isolate][name]` 一致
    /// （两侧都未隔离时同为根作用域，也相等）。
    scopes: HashMap<String, ScopeKey>,
    listener: ListenerKind,
}

/// 注册一条监听器所需的元信息（由 [`crate::Context`] 侧组装）。
pub(crate) struct HookMeta {
    id: u64,
    once: bool,
    global: bool,
    scopes: HashMap<String, ScopeKey>,
}

/// 各监听器形态的并集（与分发模式一一对应）。
enum ListenerKind {
    Observe(ObserveErased),
    Serial(SerialErased),
    Waterfall(WaterfallErased),
    SyncWaterfall(SyncWaterfallErased),
}

/// 链条/负载内部类型漂移（理论不可达的防御性分支：typed 包装器已约束类型）。
fn type_drift(where_: &str) -> CordisError {
    CordisError::PluginFailed {
        plugin: format!("{where_}: listener value type drift"),
        source: "typed wrapper invariant violated".into(),
    }
}

/// 把契约冲突翻译成模式或负载类型的精确错误。
fn contract_mismatch(event: &str, actual: Contract, expected: Contract) -> CordisError {
    if actual.kind == expected.kind {
        CordisError::EventPayloadMismatch {
            event: event.to_string(),
            actual: actual.payload,
            expected: expected.payload,
        }
    } else {
        CordisError::EventModeMismatch {
            event: event.to_string(),
            actual: kind_label(actual.kind),
            expected: kind_label(expected.kind),
        }
    }
}

/// 契约形态允许的分发方式（错误消息用）。
fn kind_label(kind: ContractKind) -> &'static str {
    match kind {
        ContractKind::Observe => "emit|parallel",
        ContractKind::Waterfall => "waterfall",
        ContractKind::Serial => "serial",
        ContractKind::SyncWaterfall => "waterfall(sync)",
    }
}

/// 瀑布事件的续体（`next`）。
///
/// 持有派发方上下文与链条中**尚未执行的**下游监听器。调用 [`Next::call`]
/// 把（可被本层改写过的）值交给下游；不调用而直接返回即**短路**——下游
/// 监听器与链尾默认行为都不会执行。这忠实对应原版“不调用 `next()` 直接
/// 返回则短路”的单决策事件语义。
///
/// `Next` 一次性消费：`call(self)` 按值获取。
pub struct Next<T> {
    ctx: Context,
    erased: NextErased,
    _marker: PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> Next<T> {
    /// 把值委托给下游监听器，返回链条最终值。
    ///
    /// # Errors
    ///
    /// 下游任一层监听器失败（沿链条向外传播）。
    pub async fn call(self, value: T) -> Result<T> {
        let boxed = (self.erased)(self.ctx, Box::new(value)).await?;
        boxed
            .downcast::<T>()
            .map(|boxed| *boxed)
            .map_err(|_| type_drift("waterfall chain value"))
    }
}

/// 同步瀑布事件的续体（`next`）。
///
/// 与 [`Next`] 同构，但整条链在调用线程上同步执行——对应原版瀑布的
/// 同步形态（Proxy trap 物理上不能异步）。语义一致：调用
/// [`SyncNext::call`] 委托下游；不调用而直接返回即**短路**。
///
/// [`SyncNext`] 一次性消费：`call(self)` 按值获取。
pub struct SyncNext<T> {
    ctx: Context,
    erased: SyncNextErased,
    _marker: PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> SyncNext<T> {
    /// 把值委托给下游监听器，返回链条最终值。
    ///
    /// # Errors
    ///
    /// 下游任一层监听器失败（沿链条向外传播）。
    pub fn call(self, value: T) -> Result<T> {
        let boxed = (self.erased)(self.ctx, Box::new(value))?;
        boxed
            .downcast::<T>()
            .map(|boxed| *boxed)
            .map_err(|_| type_drift("sync waterfall chain value"))
    }
}

/// 事件总线句柄（内置服务 `events` 的值类型）。
///
/// 用户侧 API 挂在 [`crate::Context`] 上；直接使用本类型可以绕过 fiber
/// 绑定（测试/诊断场景），此时监听器不会随 fiber 卸载自动撤销。
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<BusInner>,
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus").finish_non_exhaustive()
    }
}

impl EventBus {
    /// 创建空总线（由 [`crate::App`] 在安装内置服务时调用）。
    pub(crate) fn new(logger: Logger) -> Self {
        Self {
            inner: Arc::new(BusInner {
                records: Mutex::new(HashMap::new()),
                next_id: std::sync::atomic::AtomicU64::new(1),
                logger,
            }),
        }
    }

    // —— 注册 ——

    /// 校验既有契约并落表。
    fn insert(
        &self,
        name: &str,
        contract: Contract,
        listener: ListenerKind,
        prepend: bool,
        meta: HookMeta,
    ) -> Result<()> {
        // 锁只覆盖记录变更；监听器执行绝不在锁内（避免与分发路径死锁）
        let mut records = lock!(self.inner.records);
        let record = records
            .entry(name.to_string())
            .or_insert_with(|| EventRecord {
                contract,
                hooks: Vec::new(),
            });
        if record.contract != contract {
            return Err(contract_mismatch(name, record.contract, contract));
        }
        let HookMeta {
            id,
            once,
            global,
            scopes,
        } = meta;
        let hook = Hook {
            id,
            once,
            global,
            scopes,
            listener,
        };
        if prepend {
            record.hooks.insert(0, hook);
        } else {
            record.hooks.push(hook);
        }
        Ok(())
    }

    /// 分配下一个监听器 id。
    fn alloc_id(&self) -> u64 {
        self.inner
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// 移除一个监听器；返回是否命中（供 fiber 卸载与手动 dispose 共用）。
    ///
    /// 事件的模式契约随首次注册固化，即使监听器清空也保留记录。
    pub(crate) fn remove_listener(&self, name: &str, id: u64) -> bool {
        let mut records = lock!(self.inner.records);
        let Some(record) = records.get_mut(name) else {
            return false;
        };
        let before = record.hooks.len();
        record.hooks.retain(|hook| hook.id != id);
        before != record.hooks.len()
    }

    /// 注册观察型监听器（emit/parallel 事件的唯一监听形态）。
    ///
    /// `once` 为真时监听器在**派发快照阶段**被同步摘除（对应原版
    /// `ctx.once` 的 dispose-then-call：一次派发恰好消费一次）。
    pub(crate) fn add_observe<T, F, Fut>(
        &self,
        name: &str,
        listener: F,
        opts: ListenOptions,
        once: bool,
        scopes: HashMap<String, ScopeKey>,
    ) -> Result<u64>
    where
        T: Send + Sync + 'static,
        F: Fn(Context, Arc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let id = self.alloc_id();
        let listener = Arc::new(listener);
        let wrapper: ObserveErased =
            Arc::new(move |ctx: Context, payload: Arc<dyn Any + Send + Sync>| {
                let listener = Arc::clone(&listener);
                Box::pin(async move {
                    let typed = payload
                        .downcast::<T>()
                        .map_err(|_| type_drift("observe payload"))?;
                    listener(ctx, typed).await
                })
            });
        let meta = HookMeta {
            id,
            once,
            global: opts.global,
            scopes,
        };
        self.insert(
            name,
            Contract::observe::<T>(),
            ListenerKind::Observe(wrapper),
            opts.prepend,
            meta,
        )?;
        Ok(id)
    }

    /// 注册串行型监听器。
    pub(crate) fn add_serial<T, R, F, Fut>(
        &self,
        name: &str,
        listener: F,
        opts: ListenOptions,
        scopes: HashMap<String, ScopeKey>,
    ) -> Result<u64>
    where
        T: Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(Context, Arc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<R>> + Send + 'static,
    {
        let id = self.alloc_id();
        let listener = Arc::new(listener);
        let wrapper: SerialErased =
            Arc::new(move |ctx: Context, payload: Arc<dyn Any + Send + Sync>| {
                let listener = Arc::clone(&listener);
                Box::pin(async move {
                    let typed = payload
                        .downcast::<T>()
                        .map_err(|_| type_drift("serial payload"))?;
                    Ok(listener(ctx, typed)
                        .await
                        .map(|bailed| Box::new(bailed) as Box<dyn Any + Send>))
                })
            });
        let meta = HookMeta {
            id,
            once: false,
            global: opts.global,
            scopes,
        };
        self.insert(
            name,
            Contract::serial::<T, R>(),
            ListenerKind::Serial(wrapper),
            opts.prepend,
            meta,
        )?;
        Ok(id)
    }

    /// 注册瀑布型监听器。
    pub(crate) fn add_waterfall<T, F, Fut>(
        &self,
        name: &str,
        listener: F,
        opts: ListenOptions,
        scopes: HashMap<String, ScopeKey>,
    ) -> Result<u64>
    where
        T: Send + Sync + 'static,
        F: Fn(Context, T, Next<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let id = self.alloc_id();
        let listener = Arc::new(listener);
        let wrapper: WaterfallErased = Arc::new(
            move |ctx: Context, boxed: Box<dyn Any + Send>, next: NextErased| {
                let listener = Arc::clone(&listener);
                Box::pin(async move {
                    let value = boxed
                        .downcast::<T>()
                        .map_err(|_| type_drift("waterfall payload"))?;
                    let next = Next {
                        ctx,
                        erased: next,
                        _marker: PhantomData,
                    };
                    let out = listener(next.ctx.clone(), *value, next).await?;
                    Ok(Box::new(out) as Box<dyn Any + Send>)
                })
            },
        );
        let meta = HookMeta {
            id,
            once: false,
            global: opts.global,
            scopes,
        };
        self.insert(
            name,
            Contract::waterfall::<T>(),
            ListenerKind::Waterfall(wrapper),
            opts.prepend,
            meta,
        )?;
        Ok(id)
    }

    /// 注册同步瀑布型监听器（拦截扩展点专用形态）。
    ///
    /// 与 [`Self::add_waterfall`] 同构，仅监听器返回值不是 future——
    /// 整条链在分发调用线程上同步执行。
    pub(crate) fn add_waterfall_sync<T, F>(
        &self,
        name: &str,
        listener: F,
        opts: ListenOptions,
        scopes: HashMap<String, ScopeKey>,
    ) -> Result<u64>
    where
        T: Send + Sync + 'static,
        F: Fn(Context, T, SyncNext<T>) -> Result<T> + Send + Sync + 'static,
    {
        let id = self.alloc_id();
        let listener = Arc::new(listener);
        let wrapper: SyncWaterfallErased = Arc::new(
            move |ctx: Context, boxed: Box<dyn Any + Send>, next: SyncNextErased| {
                let value = boxed
                    .downcast::<T>()
                    .map_err(|_| type_drift("sync waterfall payload"))?;
                let next = SyncNext {
                    ctx: ctx.clone(),
                    erased: next,
                    _marker: PhantomData,
                };
                let out = listener(ctx, *value, next)?;
                Ok(Box::new(out) as Box<dyn Any + Send>)
            },
        );
        let meta = HookMeta {
            id,
            once: false,
            global: opts.global,
            scopes,
        };
        self.insert(
            name,
            Contract::waterfall_sync::<T>(),
            ListenerKind::SyncWaterfall(wrapper),
            opts.prepend,
            meta,
        )?;
        Ok(id)
    }

    // —— 分发 ——

    /// 公开事件分发前的统一观测点（对应原版 `dispatch` 内的
    /// `internal/dispatch` emit）。框架事件（`internal/*`）豁免，防递归。
    fn report_dispatch(&self, ctx: &Context, mode: DispatchMode, name: &str) {
        if name.starts_with("internal/") {
            return;
        }
        let payload = DispatchEvent {
            mode,
            name: name.to_string(),
        };
        if let Err(error) = self.emit_inner(ctx, event::DISPATCH, payload, None) {
            self.inner
                .logger
                .warn(format!("internal/dispatch 分发失败: {error}"));
        }
    }

    /// 取某事件的监听器快照并校验分发契约（锁在快照后立即释放）。
    ///
    /// `once` 监听器在此处（派发开始）同步摘除——若等监听器异步执行时再
    /// 自移除，连续两次 emit 的快照都会包含它，导致双触发。本次快照仍按
    /// 注册位置包含它们，顺序语义不变。
    ///
    /// `filter` 为 `(服务名, 作用域键)` 时，只保留通过作用域过滤的监听器
    /// （见 [`Hook::scopes`] 的判定说明）。
    fn snapshot(
        &self,
        name: &str,
        contract: Contract,
        filter: Option<(&str, ScopeKey)>,
    ) -> Result<Vec<ListenerKind>> {
        let mut records = lock!(self.inner.records);
        let Some(record) = records.get_mut(name) else {
            return Ok(Vec::new());
        };
        if record.contract != contract {
            return Err(contract_mismatch(name, record.contract, contract));
        }
        let in_scope = |hook: &Hook| match filter {
            None => true,
            Some((service, key)) => {
                hook.global || hook.scopes.get(service).copied().unwrap_or(ROOT_SCOPE) == key
            }
        };
        let snapshot: Vec<ListenerKind> = record
            .hooks
            .iter()
            .filter(|hook| in_scope(hook))
            .map(|hook| match &hook.listener {
                ListenerKind::Observe(f) => ListenerKind::Observe(Arc::clone(f)),
                ListenerKind::Serial(f) => ListenerKind::Serial(Arc::clone(f)),
                ListenerKind::Waterfall(f) => ListenerKind::Waterfall(Arc::clone(f)),
                ListenerKind::SyncWaterfall(f) => ListenerKind::SyncWaterfall(Arc::clone(f)),
            })
            .collect();
        if record.hooks.iter().any(|hook| hook.once) {
            record.hooks.retain(|hook| !hook.once);
        }
        Ok(snapshot)
    }

    /// 触发即忘地分发：每个监听器在独立任务中运行，错误记日志。
    ///
    /// 对应原版 `emit`（不 await 监听器返回的 promise）。返回值只反映
    /// 分发契约是否合法，不反映监听器结果。
    ///
    /// # Errors
    ///
    /// 事件的模式/负载类型契约不符。
    pub fn emit<T>(&self, ctx: &Context, name: &str, payload: T) -> Result<()>
    where
        T: Send + Sync + 'static,
    {
        self.emit_inner(ctx, name, payload, None)
    }

    /// [`Self::emit`] 的作用域过滤版本：只有注册方对该服务名的作用域
    /// 与 `scope` 相同（或 `global`）的监听器收到事件。
    ///
    /// 框架用于 `internal/service`：服务变更只广播给同作用域监听器，
    /// 对应原版 `reflect.notify` 携带 isolate 过滤的派发。
    ///
    /// # Errors
    ///
    /// 同 [`Self::emit`]。
    pub(crate) fn emit_scoped<T>(
        &self,
        ctx: &Context,
        name: &str,
        payload: T,
        service: &str,
        scope: ScopeKey,
    ) -> Result<()>
    where
        T: Send + Sync + 'static,
    {
        self.emit_inner(ctx, name, payload, Some((service, scope)))
    }

    /// emit 的实现：快照（可带作用域过滤）后逐监听器派发独立任务。
    fn emit_inner<T>(
        &self,
        ctx: &Context,
        name: &str,
        payload: T,
        filter: Option<(&str, ScopeKey)>,
    ) -> Result<()>
    where
        T: Send + Sync + 'static,
    {
        self.report_dispatch(ctx, DispatchMode::Emit, name);
        let hooks = self.snapshot(name, Contract::observe::<T>(), filter)?;
        let payload = Arc::new(payload) as Arc<dyn Any + Send + Sync>;
        for hook in hooks {
            let ListenerKind::Observe(listener) = hook else {
                continue; // snapshot 已保证形态一致，防御性分支
            };
            let payload = Arc::clone(&payload);
            let logger = self.inner.logger.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                if let Err(error) = listener(ctx, payload).await {
                    logger.error(format!("emit listener failed: {error}"));
                }
            });
        }
        Ok(())
    }

    /// 并发分发：所有监听器同时运行，等待全部完成。
    ///
    /// # Errors
    ///
    /// 契约不符，或若干监听器失败（汇入 [`CordisError::ParallelDispatchFailed`]）。
    pub async fn parallel<T>(&self, ctx: &Context, name: &str, payload: T) -> Result<()>
    where
        T: Send + Sync + 'static,
    {
        self.report_dispatch(ctx, DispatchMode::Parallel, name);
        let hooks = self.snapshot(name, Contract::observe::<T>(), None)?;
        let payload = Arc::new(payload) as Arc<dyn Any + Send + Sync>;
        // 监听器 future 全部 'static：用独立任务实现真正的并发扇出
        let mut tasks = Vec::with_capacity(hooks.len());
        for hook in hooks {
            let ListenerKind::Observe(listener) = hook else {
                continue;
            };
            let payload = Arc::clone(&payload);
            let ctx = ctx.clone();
            tasks.push(tokio::spawn(async move { listener(ctx, payload).await }));
        }
        let mut errors: Vec<CordisError> = Vec::new();
        for task in tasks {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error),
                Err(join) => errors.push(CordisError::PluginFailed {
                    plugin: "<parallel listener panicked>".into(),
                    source: Box::new(join),
                }),
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            let count = errors.len();
            Err(CordisError::ParallelDispatchFailed {
                event: name.to_string(),
                count,
                sources: errors,
            })
        }
    }

    /// 串行分发：按注册顺序执行，首个返回 `Some` 的监听器短路。
    ///
    /// # Errors
    ///
    /// 契约不符，或链条内部类型漂移（防御性分支）。
    pub async fn serial<T, R>(&self, ctx: &Context, name: &str, payload: T) -> Result<Option<R>>
    where
        T: Send + Sync + 'static,
        R: Send + 'static,
    {
        self.report_dispatch(ctx, DispatchMode::Serial, name);
        let hooks = self.snapshot(name, Contract::serial::<T, R>(), None)?;
        let payload = Arc::new(payload) as Arc<dyn Any + Send + Sync>;
        for hook in hooks {
            let ListenerKind::Serial(listener) = hook else {
                continue;
            };
            let Some(bailed) = listener(ctx.clone(), Arc::clone(&payload)).await? else {
                continue;
            };
            return bailed
                .downcast::<R>()
                .map(|boxed| Some(*boxed))
                .map_err(|_| type_drift("serial bail value"));
        }
        Ok(None)
    }

    /// 瀑布分发：监听器按注册顺序层层包装，返回链条最终值。
    ///
    /// 先注册者在最外层；监听器不调用 [`Next::call`] 即短路。
    ///
    /// # Errors
    ///
    /// 契约不符，或任一层监听器失败（沿链条向外传播）。
    pub async fn waterfall<T>(&self, ctx: &Context, name: &str, value: T) -> Result<T>
    where
        T: Send + Sync + 'static,
    {
        self.report_dispatch(ctx, DispatchMode::Waterfall, name);
        let hooks = self.snapshot(name, Contract::waterfall::<T>(), None)?;
        let chain: Vec<WaterfallErased> = hooks
            .into_iter()
            .filter_map(|hook| match hook {
                ListenerKind::Waterfall(f) => Some(f),
                _ => None,
            })
            .collect();
        let next = Self::link_from(chain.into_iter());
        let boxed = next(ctx.clone(), Box::new(value)).await?;
        boxed
            .downcast::<T>()
            .map(|boxed| *boxed)
            .map_err(|_| type_drift("waterfall final value"))
    }

    /// 把瀑布监听器迭代器编成一条擦除续体链（先注册者先执行，链尾恒等）。
    fn link_from(mut iter: std::vec::IntoIter<WaterfallErased>) -> NextErased {
        Box::new(
            move |ctx: Context, boxed: Box<dyn Any + Send>| match iter.next() {
                Some(listener) => {
                    let rest = Self::link_from(iter);
                    Box::pin(async move { listener(ctx, boxed, rest).await })
                }
                None => Box::pin(async move { Ok(boxed) }),
            },
        )
    }

    /// 同步瀑布分发：与 [`Self::waterfall`] 同构，整条链同步执行。
    ///
    /// 框架用于 `internal/get` / `internal/set` 拦截（对应原版 Proxy trap
    /// 内的同步 waterfall）。快照释放 records 锁后才走链，监听器执行
    /// 不持任何锁——链内嵌套 `get` / `provide` 安全。
    ///
    /// # Errors
    ///
    /// 契约不符，或任一层监听器失败（沿链条向外传播）。
    pub fn waterfall_sync<T>(&self, ctx: &Context, name: &str, value: T) -> Result<T>
    where
        T: Send + Sync + 'static,
    {
        let hooks = self.snapshot(name, Contract::waterfall_sync::<T>(), None)?;
        let chain: Vec<SyncWaterfallErased> = hooks
            .into_iter()
            .filter_map(|hook| match hook {
                ListenerKind::SyncWaterfall(f) => Some(f),
                _ => None,
            })
            .collect();
        let next = Self::link_from_sync(chain.into_iter());
        let boxed = next(ctx.clone(), Box::new(value))?;
        boxed
            .downcast::<T>()
            .map(|boxed| *boxed)
            .map_err(|_| type_drift("sync waterfall final value"))
    }

    /// 把同步瀑布监听器迭代器编成一条擦除续体链（链尾恒等）。
    fn link_from_sync(mut iter: std::vec::IntoIter<SyncWaterfallErased>) -> SyncNextErased {
        Box::new(
            move |ctx: Context, boxed: Box<dyn Any + Send>| match iter.next() {
                Some(listener) => {
                    let rest = Self::link_from_sync(iter);
                    listener(ctx, boxed, rest)
                }
                None => Ok(boxed),
            },
        )
    }
}
