//! Context：服务容器与 App：共享运行时。
//!
//! 对应原版 `Context` 类与 context proxy。原版用 prototype 链 + Proxy 让
//! `ctx.<key>` 动态解析服务；Rust 版把同一语义落成：
//!
//! - **[`Context`] 是廉价的克隆句柄**（`Arc` 内部状态），代表"某个 fiber
//!   视角下的容器切片"——沿着 `parent` 链向上解析服务，沿 `isolate` 覆盖
//!   隔离作用域；
//! - **[`App`] 持有共享运行时**（服务存储、注册表、事件总线、日志），
//!   与根 fiber 一起构成整棵插件树的锚点；
//! - 服务以 **名字为键**、以 `Arc<dyn Any>` 为值存储：`ctx.provide`
//!   写入、`ctx.get::<T>` 读取并向下转型——"其他插件通过 key 查找服务，
//!   而非导入具体实现"。
//!
//! ## 与原版 proxy 语义的差异
//!
//! 原版 `ctx.foo` 属性访问即服务读取；Rust 没有动态属性，服务读取统一
//! 走 `ctx.get::<T>("foo")`。未声明 `inject` 就读取服务在原版是运行时
//! 报错，Rust 版将"读取"与"依赖等待"解耦：`get` 返回 `Option`，
//! "等待就绪再启动"的语义由 [`crate::Plugin::inject`] 全权承担。

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::task::JoinHandle;

use crate::error::{CordisError, Result};
use crate::events::EventBus;
use crate::fiber::{Fiber, FiberInner, FiberState};
use crate::logger::Logger;
use crate::registry::{Plugin, Registry};
use crate::{ListenOptions, ROOT_SCOPE, ScopeKey, event};

/// 服务值在存储中的擦除形态。
type AnyService = Arc<dyn Any + Send + Sync>;

/// 一条服务实现记录（对应原版 `ReflectService.Impl`）。
struct ServiceImpl {
    /// 服务值。
    value: AnyService,
    /// 注册时记录的负载类型名（`expect` 向下转型失败时的诊断）。
    type_name: &'static str,
    /// 提供方 fiber（弱引用：fiber 由注册表与父级保活）。
    fiber: Weak<FiberInner>,
}

/// `internal/service` 事件的负载。
#[derive(Clone)]
pub struct ServiceEvent {
    /// 变化的服务名。
    pub name: String,
    /// 新的服务值；撤销时为 `None`。
    pub value: Option<AnyService>,
}

/// 向下转型：成功给值，失败给带注册类型名的精确错误。
fn value_or_type_error<T>(value: &AnyService, actual: &'static str, name: &str) -> Result<Arc<T>>
where
    T: Send + Sync + 'static,
{
    match Arc::clone(value).downcast::<T>() {
        Ok(typed) => Ok(typed),
        Err(_) => Err(CordisError::ServiceTypeMismatch {
            name: name.to_string(),
            actual,
            expected: std::any::type_name::<T>(),
        }),
    }
}

impl std::fmt::Debug for ServiceEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceEvent")
            .field("name", &self.name)
            .field("value", &self.value.as_ref().map(|_| "<service>"))
            .finish()
    }
}

/// App 共享运行时（所有上下文通过 `Arc` 共享）。
pub(crate) struct AppInner {
    /// 服务存储：(隔离作用域, 服务名) → 实现记录。
    services: Mutex<HashMap<(ScopeKey, String), ServiceImpl>>,
    /// 插件注册表。
    registry: Mutex<Registry>,
    /// 事件总线（内置服务 `events` 的本体）。
    pub(crate) bus: EventBus,
    /// 框架日志器。
    pub(crate) logger: Logger,
    /// 销毁任务清单（shutdown 逐一等待）。
    pending_disposals: Mutex<Vec<JoinHandle<()>>>,
    /// 具名隔离作用域 label → key。
    scope_labels: Mutex<HashMap<String, ScopeKey>>,
    next_scope: AtomicU64,
    next_uid: AtomicU64,
}

impl AppInner {
    /// 摘除一个 fiber 的全部注册记录（销毁路径）。
    pub(crate) fn unmount_fiber(&self, fiber: &Arc<FiberInner>) {
        if let Some(uid) = fiber.uid() {
            lock!(self.registry).unmount(fiber.plugin(), uid);
        }
    }

    /// 登记一个销毁任务，供 shutdown 等待。
    pub(crate) fn push_pending_disposal(&self, task: JoinHandle<()>) {
        lock!(self.pending_disposals).push(task);
    }

    /// 列出某 fiber 提供的全部服务名（进入 Active 时唤醒下游用）。
    pub(crate) fn services_provided_by(&self, fiber: &Arc<FiberInner>) -> Vec<String> {
        let services = lock!(self.services);
        services
            .iter()
            .filter(|(_, impl_)| impl_.fiber.as_ptr() == Arc::as_ptr(fiber))
            .map(|((_, name), _)| name.clone())
            .collect()
    }

    /// 服务变化通知：唤醒所有注入了该服务且作用域可达的 fiber。
    pub(crate) fn notify_service_change(&self, name: &str) {
        let candidates = {
            let registry = lock!(self.registry);
            registry
                .all_fibers()
                .into_iter()
                .filter(|fiber| fiber.injects(name))
                .collect::<Vec<_>>()
        };
        for fiber in candidates {
            fiber.refresh();
        }
    }

    /// 注册表中的运行时记录数。
    pub(crate) fn plugin_count(&self) -> usize {
        lock!(self.registry).size()
    }

    /// 各运行时记录的展示名。
    pub(crate) fn plugin_names(&self) -> Vec<String> {
        lock!(self.registry).runtime_names()
    }

    /// 列出全部 fiber 句柄与状态（诊断）。
    pub(crate) fn fiber_states(&self) -> Vec<(String, FiberState)> {
        lock!(self.registry)
            .all_fibers()
            .into_iter()
            .map(|fiber| (fiber.display_name().to_string(), fiber.state()))
            .collect()
    }

    /// 全部活跃 fiber 的公开句柄（`RegistryService` 的数据源）。
    pub(crate) fn fiber_handles(&self) -> Vec<Fiber> {
        lock!(self.registry)
            .all_fibers()
            .into_iter()
            .map(Fiber)
            .collect()
    }

    /// 分配下一个 fiber uid。
    fn alloc_uid(&self) -> u64 {
        self.next_uid.fetch_add(1, Ordering::Relaxed)
    }

    /// 取（或创建）具名隔离作用域。
    fn scope_of_label(&self, label: &str) -> ScopeKey {
        let mut labels = lock!(self.scope_labels);
        if let Some(key) = labels.get(label) {
            return *key;
        }
        let key = self.next_scope.fetch_add(1, Ordering::Relaxed);
        labels.insert(label.to_string(), key);
        key
    }

    /// 分配新的匿名隔离作用域。
    fn fresh_scope(&self) -> ScopeKey {
        self.next_scope.fetch_add(1, Ordering::Relaxed)
    }
}

/// 插件注册表的公开句柄（内置服务 `registry` 的值类型）。
///
/// 对应原版 `ctx.registry`（RegistryService）：任何插件都能枚举运行时
/// 记录与 fiber 状态——教程用它诊断"永远 PENDING 的插件缺哪个服务"。
/// 持弱引用避免与 `AppInner` 成环（App 句柄才是强锚点），App 释放后
/// 查询返回空值。
#[derive(Clone)]
pub struct RegistryService {
    app: Weak<AppInner>,
}

impl std::fmt::Debug for RegistryService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryService").finish_non_exhaustive()
    }
}

impl RegistryService {
    /// 由 `App::new` 构造（框架内部使用）。
    fn of(app: &Arc<AppInner>) -> Self {
        Self {
            app: Arc::downgrade(app),
        }
    }

    /// 升级共享运行时；App 已释放时返回 `None`。
    fn app(&self) -> Option<Arc<AppInner>> {
        self.app.upgrade()
    }

    /// 注册表中的运行时记录数（对应原版 `registry.size`）。
    #[must_use]
    pub fn size(&self) -> usize {
        self.app().map_or(0, |app| app.plugin_count())
    }

    /// 各运行时记录的展示名（对应原版 `registry.keys` 的名字视图）。
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.app().map_or_else(Vec::new, |app| app.plugin_names())
    }

    /// 全部活跃 fiber 的句柄（对应原版遍历 `runtime.fibers`）。
    #[must_use]
    pub fn fibers(&self) -> Vec<Fiber> {
        self.app().map_or_else(Vec::new, |app| app.fiber_handles())
    }

    /// 处于 `Pending` 状态的 fiber（依赖未就绪的诊断入口）。
    ///
    /// 配合 [`Fiber::missing_dependencies`] 即可回答"谁在等什么"。
    #[must_use]
    pub fn pending(&self) -> Vec<Fiber> {
        self.fibers()
            .into_iter()
            .filter(|fiber| fiber.state() == FiberState::Pending)
            .collect()
    }
}

/// 应用：插件树的入口与共享运行时的持有者。
///
/// # Examples
///
/// ```
/// use cordis::{fn_plugin, App, Context, Result};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let app = App::new()?;
///     app.root().plugin(fn_plugin("hello", &[], |ctx: Context| async {
///         println!("hello from plugin");
///         Ok(())
///     }))?;
///     app.shutdown().await;
///     Ok(())
/// }
/// ```
pub struct App {
    inner: Arc<AppInner>,
    root_fiber: Arc<FiberInner>,
}

impl App {
    /// 创建应用：构造根上下文、根 fiber，并安装内置服务。
    ///
    /// # Errors
    ///
    /// 内置服务安装失败（理论上不会发生）。
    pub fn new() -> Result<Self> {
        let logger = Logger::new("cordis");
        let bus = EventBus::new(logger.clone());
        // 框架内部日志与 "logger" 服务共用同一实例：服务形态让它可寻址
        let inner = Arc::new(AppInner {
            services: Mutex::new(HashMap::new()),
            registry: Mutex::new(Registry::new()),
            bus: bus.clone(),
            logger: logger.clone(),
            pending_disposals: Mutex::new(Vec::new()),
            scope_labels: Mutex::new(HashMap::new()),
            next_scope: AtomicU64::new(1),
            next_uid: AtomicU64::new(1),
        });

        // 根上下文与根 fiber 互相引用：ctx 持弱引用，App 持强引用
        let root_ctx = Context::bare(Arc::clone(&inner));
        let root_fiber = FiberInner::root(Arc::downgrade(&inner));
        root_ctx.bind_fiber(Arc::downgrade(&root_fiber));
        root_fiber.set_ctx(root_ctx);

        let app = Self { inner, root_fiber };

        // 内置服务自举（对应原版构造函数里安装 events/logger/reflect/registry）：
        // 框架自身的能力以服务形态挂到根上下文，可寻址、可注入——
        // "不存在需要打补丁的特权内核"从这里开始。三个名字均为框架独占，
        // 冲突即框架 bug，向上如实传播。
        app.root().provide("events", bus)?;
        app.root()
            .provide("registry", RegistryService::of(&app.inner))?;
        app.root().provide("logger", logger.clone())?;
        Ok(app)
    }

    /// 根上下文（每个子上下文都从它派生）。
    #[must_use]
    pub fn root(&self) -> Context {
        self.root_fiber.ctx()
    }

    /// 根 fiber。
    #[must_use]
    pub fn root_fiber(&self) -> Fiber {
        Fiber(Arc::clone(&self.root_fiber))
    }

    /// 注册表中的运行时记录数（诊断）。
    #[must_use]
    pub fn plugin_count(&self) -> usize {
        self.inner.plugin_count()
    }

    /// 各运行时记录的展示名（诊断）。
    #[must_use]
    pub fn plugin_names(&self) -> Vec<String> {
        self.inner.plugin_names()
    }

    /// 全部 fiber 的 (名字, 状态) 快照（诊断）。
    #[must_use]
    pub fn fiber_states(&self) -> Vec<(String, FiberState)> {
        self.inner.fiber_states()
    }

    /// 优雅停机：销毁根 fiber，并等待全部销毁任务收尾。
    ///
    /// 根 fiber 的副作用表按逆序清空，因此子插件先于其挂载者清理——
    /// 整棵树的卸载顺序恰与挂载顺序相反。
    pub async fn shutdown(&self) {
        self.root_fiber.begin_dispose();
        loop {
            let tasks: Vec<JoinHandle<()>> = {
                let mut pending = lock!(self.inner.pending_disposals);
                std::mem::take(&mut *pending)
            };
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                let _ = task.await;
            }
        }
    }
}

/// 上下文内部状态。
struct ContextInner {
    app: Arc<AppInner>,
    fiber: std::sync::OnceLock<Weak<FiberInner>>,
    parent: Option<Context>,
    /// 本层隔离覆盖：(服务名, 作用域键)。
    isolate: Option<(String, ScopeKey)>,
}

/// 上下文：服务容器的克隆句柄，代表某个 fiber 的视角。
///
/// 子上下文由框架创建（挂载插件、`isolate` 派生），用户代码只克隆。
#[derive(Clone)]
pub struct Context {
    inner: Arc<ContextInner>,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fiber_name = self.inner.fiber.get().and_then(Weak::upgrade).map_or_else(
            || "<unbound>".into(),
            |fiber| fiber.display_name().to_string(),
        );
        write!(f, "Context <{fiber_name}>")
    }
}

impl Context {
    /// 构造尚未绑定 fiber 的裸上下文（仅 `App::new` 使用）。
    fn bare(app: Arc<AppInner>) -> Self {
        Self {
            inner: Arc::new(ContextInner {
                app,
                fiber: std::sync::OnceLock::new(),
                parent: None,
                isolate: None,
            }),
        }
    }

    /// 绑定 fiber（仅根上下文在构造后补写）。
    fn bind_fiber(&self, fiber: Weak<FiberInner>) {
        let _ = self.inner.fiber.set(fiber);
    }

    /// 在父上下文之上派生 fiber 上下文（挂载插件时框架调用）。
    fn for_fiber(parent: &Context, fiber: &Arc<FiberInner>) -> Self {
        Self {
            inner: Arc::new(ContextInner {
                app: Arc::clone(&parent.inner.app),
                fiber: std::sync::OnceLock::from(Arc::downgrade(fiber)),
                parent: Some(parent.clone()),
                isolate: None,
            }),
        }
    }

    /// 共享运行时。
    fn app(&self) -> &Arc<AppInner> {
        &self.inner.app
    }

    /// 当前上下文归属的 fiber。
    ///
    /// # Errors
    ///
    /// fiber 已被释放（App 与全部上下文均已销毁）。
    pub fn fiber(&self) -> Result<Fiber> {
        self.inner
            .fiber
            .get()
            .and_then(Weak::upgrade)
            .map(Fiber)
            .ok_or(CordisError::InactiveContext)
    }

    /// 沿上下文链解析服务名的作用域键（最内层 isolate 覆盖优先）。
    fn scope_key_for(&self, name: &str) -> ScopeKey {
        let mut cursor = Some(self);
        while let Some(ctx) = cursor {
            if let Some((isolated, key)) = &ctx.inner.isolate
                && isolated == name
            {
                return *key;
            }
            cursor = ctx.inner.parent.as_ref();
        }
        ROOT_SCOPE
    }

    /// 注册监听器时捕获的隔离快照（服务名 → 作用域键，内层覆盖外层）。
    ///
    /// 上下文链在构造后不可变，快照在注册时刻即终态。事件总线用它做
    /// `internal/service` 的作用域过滤（见 `events::Hook`）。
    fn isolate_snapshot(&self) -> HashMap<String, ScopeKey> {
        let mut scopes = HashMap::new();
        let mut cursor = Some(self);
        while let Some(ctx) = cursor {
            if let Some((name, key)) = &ctx.inner.isolate {
                scopes.entry(name.clone()).or_insert(*key);
            }
            cursor = ctx.inner.parent.as_ref();
        }
        scopes
    }

    /// 解析服务实现：查作用域 + 要求提供方 Active（严格模式）。
    pub(crate) fn resolve_service_impl(&self, name: &str) -> Option<AnyService> {
        let key = self.scope_key_for(name);
        let app = &self.inner.app;
        let services = lock!(app.services);
        let impl_ = services.get(&(key, name.to_string()))?;
        let provider = impl_.fiber.upgrade()?;
        if provider.state() != FiberState::Active {
            return None;
        }
        Some(Arc::clone(&impl_.value))
    }

    // —— 服务 ——

    /// 提供一个服务（当前 fiber 拥有其生命周期）。
    ///
    /// 服务名在当前隔离作用域内必须未被占用。返回的 [`crate::Disposer`]
    /// 可提前撤销；否则随 fiber 卸载自动撤销。撤销会唤醒所有依赖该服务
    /// 的下游 fiber 重新评估依赖。
    ///
    /// # Errors
    ///
    /// 当前 fiber 已失活、或服务名已被占用。
    pub fn provide<T>(&self, name: &str, value: T) -> Result<crate::Disposer>
    where
        T: Send + Sync + 'static,
    {
        self.provide_arc(name, Arc::new(value))
    }

    /// [`Self::provide`] 的共享引用版本（服务实例被多方持有时避免复制）。
    ///
    /// # Errors
    ///
    /// 同 [`Self::provide`]。
    pub fn provide_arc<T>(&self, name: &str, value: Arc<T>) -> Result<crate::Disposer>
    where
        T: Send + Sync + 'static,
    {
        let fiber = self.fiber()?.0;
        fiber.assert_active()?;
        let key = self.scope_key_for(name);
        let app = self.app();
        // 广播用副本：本体 move 进存储，避免"借而不取"
        let shared = Arc::clone(&value) as AnyService;
        {
            let mut services = lock!(app.services);
            if let Some(existing) = services.get(&(key, name.to_string())) {
                let owner = existing
                    .fiber
                    .upgrade()
                    .map_or_else(|| "<gone>".into(), |f| f.display_name().to_string());
                return Err(CordisError::AlreadyProvided {
                    name: name.to_string(),
                    owner,
                });
            }
            services.insert(
                (key, name.to_string()),
                ServiceImpl {
                    value,
                    type_name: std::any::type_name::<T>(),
                    fiber: Arc::downgrade(&fiber),
                },
            );
        }

        // 广播 + 唤醒下游（提供方须已 Active，Loading 期间的提供由
        // 状态切换到 Active 时的 notify_provided 统一广播）
        if fiber.state() == FiberState::Active {
            self.announce_service(name, Some(shared));
        }
        let name_owned = name.to_string();
        let app = Arc::clone(app);
        let ctx_self = self.clone();
        let (slot, disposer) = crate::effect::Disposer::from_cleanup(
            format!("ctx.provide({name:?})"),
            Some(Box::new(move || {
                let mut services = lock!(app.services);
                services.remove(&(key, name_owned.clone()));
                drop(services);
                ctx_self.announce_service(&name_owned, None);
                app.notify_service_change(&name_owned);
            })),
        );
        fiber.push_effect(format!("ctx.provide({name:?})"), slot)?;
        Ok(disposer)
    }

    /// 广播 `internal/service`（按服务作用域过滤，对应原版 `reflect.notify`
    /// 携带 isolate 过滤的派发：只有同作用域或 `global` 的监听器收到）。
    pub(crate) fn announce_service(&self, name: &str, value: Option<AnyService>) {
        let payload = ServiceEvent {
            name: name.to_string(),
            value,
        };
        let scope = self.scope_key_for(name);
        if let Err(error) = self
            .app()
            .bus
            .emit_scoped(self, event::SERVICE, payload, name, scope)
        {
            self.app()
                .logger
                .warn(format!("internal/service 分发失败: {error}"));
        }
    }

    /// 读取服务；未提供（或提供方未就绪）时返回 `None`。
    ///
    /// "读取"与"依赖等待"解耦：需要等待语义时应声明
    /// [`Plugin::inject`](crate::Plugin::inject)。
    #[must_use]
    pub fn get<T>(&self, name: &str) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let value = self.resolve_service_impl(name)?;
        value.downcast::<T>().ok()
    }

    /// 读取服务，类型不符或未提供时给出精确错误。
    ///
    /// # Errors
    ///
    /// [`CordisError::ServiceNotProvided`] 或 [`CordisError::ServiceTypeMismatch`]。
    pub fn expect<T>(&self, name: &str) -> Result<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let key = self.scope_key_for(name);
        let app = &self.inner.app;
        {
            let services = lock!(app.services);
            if let Some(impl_) = services.get(&(key, name.to_string())) {
                return value_or_type_error(&impl_.value, impl_.type_name, name);
            }
        }
        // 提供方未就绪（或未提供）统一视作不可用
        Err(CordisError::ServiceNotProvided(name.to_string()))
    }

    // —— 插件 ——

    /// 挂载一个插件（按值传入，配置在构造期捕获）。
    ///
    /// 返回 fiber 句柄；`apply` 在后台异步运行，用 [`Fiber::ready`] 等待。
    /// fiber 的存活登记为当前 fiber 的副作用：当前 fiber 卸载时子 fiber
    /// 一并卸载。
    ///
    /// # Errors
    ///
    /// 当前 fiber 已失活。
    pub fn plugin<P: Plugin>(&self, plugin: P) -> Result<Fiber> {
        let plugin: Arc<dyn Plugin> = Arc::new(plugin);
        self.plugin_dyn(&plugin)
    }

    /// [`Self::plugin`] 的已装箱版本（同一实例重复挂载共享运行时记录）。
    ///
    /// # Errors
    ///
    /// 同 [`Self::plugin`]。
    pub fn plugin_dyn(&self, plugin: &Arc<dyn Plugin>) -> Result<Fiber> {
        let parent_fiber = self.fiber()?;
        parent_fiber.0.assert_active()?;

        let app = Arc::clone(self.app());
        let uid = app.alloc_uid();
        let name = plugin.name().to_string();
        let inject = plugin.inject();
        let fiber = FiberInner::plugin_fiber(
            Arc::downgrade(&app),
            Arc::clone(plugin),
            name.clone(),
            inject,
            uid,
        );
        let fctx = Context::for_fiber(self, &fiber);
        fiber.set_ctx(fctx);

        // 1) 注册进运行时记录
        lock!(app.registry).mount(plugin, Arc::clone(&fiber));

        // 2) 子 fiber 的生命周期登记为父 fiber 的副作用（逆序卸载）
        let child = Arc::clone(&fiber);
        let (slot, _child_disposer) = crate::effect::Disposer::from_cleanup(
            format!("ctx.plugin({name:?})"),
            Some(Box::new(move || child.begin_dispose())),
        );
        parent_fiber
            .0
            .push_effect(format!("ctx.plugin({name:?})"), slot)?;

        // 3) 发布 internal/plugin（先于依赖解析，对应原版构造顺序）
        if let Err(error) = app.bus.emit(self, event::PLUGIN, Fiber(Arc::clone(&fiber))) {
            app.logger
                .warn(format!("internal/plugin 分发失败: {error}"));
        }

        // 4) 解析依赖，需要时驱动加载
        fiber.refresh();
        Ok(Fiber(fiber))
    }

    /// 挂载闭包插件并等待其依赖（对应原版 `ctx.inject(deps, callback)`）。
    ///
    /// # Errors
    ///
    /// 同 [`Self::plugin`]。
    pub fn inject<F, Fut>(&self, deps: &[&str], callback: F) -> Result<Fiber>
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.plugin(crate::fn_plugin("injected", deps, callback))
    }

    // —— 副作用 ——

    /// 注册一个可逆副作用：`cleanup` 在副作用撤销时执行。
    ///
    /// 对应原版 `ctx.effect(execute)`——原版 setup 立即执行并收集返回的
    /// disposer；Rust 版把"setup"留给调用方在注册前完成，本方法只登记
    /// 清理动作。副作用按**逆注册顺序**撤销，早调用 `dispose` 可单独提前
    /// 撤销（与 fiber 卸载竞争时只执行一次）。
    ///
    /// # Errors
    ///
    /// 当前 fiber 已失活。
    pub fn effect(
        &self,
        label: impl Into<String>,
        cleanup: Option<crate::effect::Cleanup>,
    ) -> Result<crate::Disposer> {
        let fiber = self.fiber()?;
        fiber.0.assert_active()?;
        let label = label.into();
        let (slot, disposer) = crate::effect::Disposer::from_cleanup(label.clone(), cleanup);
        fiber.0.push_effect(label, slot)?;
        Ok(disposer)
    }

    /// 以 RAII 守卫形态注册副作用：守卫 **Drop 即撤销**（Rust 母语）。
    ///
    /// 与 [`Self::effect`]（JS 语义：句柄丢弃 ≠ 效果撤销）注册的是同一套
    /// 副作用槽位，只是返回形态不同。需要 Cordis 默认语义时对返回值调用
    /// [`crate::EffectGuard::disarm`]。
    ///
    /// # Errors
    ///
    /// 当前 fiber 已失活。
    pub fn guard(
        &self,
        label: impl Into<String>,
        cleanup: Option<crate::effect::Cleanup>,
    ) -> Result<crate::EffectGuard> {
        Ok(crate::EffectGuard::wrap(self.effect(label, cleanup)?))
    }

    // —— 事件 ——

    /// 注册观察型监听器（emit / parallel 事件）。
    ///
    /// 监听器收到 `(派发方上下文, 负载)`；负载以 `Arc` 共享，观察者不应
    /// 修改（需要可变负载时用 `waterfall` 或内部可变性）。
    ///
    /// # Errors
    ///
    /// 事件的模式/负载类型契约不符，或当前 fiber 已失活。
    pub fn on<T, F, Fut>(&self, name: &str, listener: F) -> Result<crate::Disposer>
    where
        T: Send + Sync + 'static,
        F: Fn(Context, Arc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_opt(name, listener, ListenOptions::default())
    }

    /// [`Self::on`] 带选项版本（`prepend` 让监听器先于既有监听器执行）。
    ///
    /// # Errors
    ///
    /// 同 [`Self::on`]。
    pub fn on_opt<T, F, Fut>(
        &self,
        name: &str,
        listener: F,
        options: ListenOptions,
    ) -> Result<crate::Disposer>
    where
        T: Send + Sync + 'static,
        F: Fn(Context, Arc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.fiber()?.0.assert_active()?;
        let id = self.app().bus.add_observe::<T, F, Fut>(
            name,
            listener,
            options,
            false,
            self.isolate_snapshot(),
        )?;
        self.wrap_listener_removal(name, id)
    }

    /// 注册只触发一次的观察型监听器（对应原版 `ctx.once`）。
    ///
    /// # Errors
    ///
    /// 同 [`Self::on`]。
    pub fn once<T, F, Fut>(&self, name: &str, listener: F) -> Result<crate::Disposer>
    where
        T: Send + Sync + 'static,
        F: Fn(Context, Arc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.fiber()?.0.assert_active()?;
        let id = self.app().bus.add_observe::<T, F, Fut>(
            name,
            listener,
            ListenOptions::default(),
            true,
            self.isolate_snapshot(),
        )?;
        self.wrap_listener_removal(name, id)
    }

    /// 注册瀑布型监听器（waterfall 事件）。
    ///
    /// 监听器收到 `(派发方上下文, 值, next)`；改写值后必须 `next.call(value)`
    /// 委托下游，否则短路。仅当监听器必须先于普通注册运行时设置 `prepend`。
    ///
    /// # Errors
    ///
    /// 事件的模式/负载类型契约不符，或当前 fiber 已失活。
    pub fn on_waterfall<T, F, Fut>(&self, name: &str, listener: F) -> Result<crate::Disposer>
    where
        T: Send + Sync + 'static,
        F: Fn(Context, T, crate::events::Next<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.on_waterfall_opt(name, listener, ListenOptions::default())
    }

    /// [`Self::on_waterfall`] 带选项版本。
    ///
    /// # Errors
    ///
    /// 同 [`Self::on_waterfall`]。
    pub fn on_waterfall_opt<T, F, Fut>(
        &self,
        name: &str,
        listener: F,
        options: ListenOptions,
    ) -> Result<crate::Disposer>
    where
        T: Send + Sync + 'static,
        F: Fn(Context, T, crate::events::Next<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.fiber()?.0.assert_active()?;
        let id = self.app().bus.add_waterfall::<T, F, Fut>(
            name,
            listener,
            options,
            self.isolate_snapshot(),
        )?;
        self.wrap_listener_removal(name, id)
    }

    /// 注册串行型监听器（serial 事件）：返回 `Some` 即决策并短路。
    ///
    /// # Errors
    ///
    /// 事件的模式/负载类型契约不符，或当前 fiber 已失活。
    pub fn on_serial<T, R, F, Fut>(&self, name: &str, listener: F) -> Result<crate::Disposer>
    where
        T: Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(Context, Arc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<R>> + Send + 'static,
    {
        self.on_serial_opt(name, listener, ListenOptions::default())
    }

    /// [`Self::on_serial`] 带选项版本。
    ///
    /// # Errors
    ///
    /// 同 [`Self::on_serial`]。
    pub fn on_serial_opt<T, R, F, Fut>(
        &self,
        name: &str,
        listener: F,
        options: ListenOptions,
    ) -> Result<crate::Disposer>
    where
        T: Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(Context, Arc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<R>> + Send + 'static,
    {
        self.fiber()?.0.assert_active()?;
        let id = self.app().bus.add_serial::<T, R, F, Fut>(
            name,
            listener,
            options,
            self.isolate_snapshot(),
        )?;
        self.wrap_listener_removal(name, id)
    }

    /// 把监听器移除动作登记为当前 fiber 的副作用。
    fn wrap_listener_removal(&self, name: &str, id: u64) -> Result<crate::Disposer> {
        let bus = self.app().bus.clone();
        let label = format!("ctx.on({name:?})");
        let name = name.to_string();
        let (slot, disposer) = crate::effect::Disposer::from_cleanup(
            label.clone(),
            Some(Box::new(move || {
                bus.remove_listener(&name, id);
            })),
        );
        self.fiber()?.0.push_effect(label, slot)?;
        Ok(disposer)
    }

    /// 触发即忘地分发（对应原版 `emit`）。
    ///
    /// # Errors
    ///
    /// 事件契约不符。
    pub fn emit<T>(&self, name: &str, payload: T) -> Result<()>
    where
        T: Send + Sync + 'static,
    {
        self.app().bus.emit(self, name, payload)
    }

    /// 并发分发并等待全部监听器（对应原版 `parallel`）。
    ///
    /// # Errors
    ///
    /// 事件契约不符，或若干监听器失败。
    pub async fn parallel<T>(&self, name: &str, payload: T) -> Result<()>
    where
        T: Send + Sync + 'static,
    {
        self.app().bus.parallel(self, name, payload).await
    }

    /// 串行分发，首个 `Some` 决策短路（对应原版 `serial`）。
    ///
    /// # Errors
    ///
    /// 事件契约不符。
    pub async fn serial<T, R>(&self, name: &str, payload: T) -> Result<Option<R>>
    where
        T: Send + Sync + 'static,
        R: Send + 'static,
    {
        self.app().bus.serial(self, name, payload).await
    }

    /// 瀑布分发（对应原版 `waterfall`）。
    ///
    /// # Errors
    ///
    /// 事件契约不符，或任一层监听器失败。
    pub async fn waterfall<T>(&self, name: &str, value: T) -> Result<T>
    where
        T: Send + Sync + 'static,
    {
        self.app().bus.waterfall(self, name, value).await
    }

    // —— 作用域 ——

    /// 派生一个对 `name` 独立作用域的子上下文。
    ///
    /// 在返回的上下文之下，`name` 的读写都落在新作用域：可以在不影响
    /// 父作用域的情况下提供另一套实现（对应原版 `ctx.isolate`）。
    #[must_use]
    pub fn isolate(&self, name: &str) -> Context {
        self.isolate_as(name, &format!("{name}:{}", self.app().fresh_scope()))
    }

    /// [`Self::isolate`] 的具名版本：同名 label 的派生共享同一作用域
    /// （对应原版 `isolate(name, label)` 的 scope join 语义）。
    #[must_use]
    pub fn isolate_as(&self, name: &str, label: &str) -> Context {
        let key = self.app().scope_of_label(label);
        Self {
            inner: Arc::new(ContextInner {
                app: Arc::clone(&self.inner.app),
                fiber: std::sync::OnceLock::from(
                    self.inner.fiber.get().cloned().unwrap_or_default(),
                ),
                parent: Some(self.clone()),
                isolate: Some((name.to_string(), key)),
            }),
        }
    }

    // —— 其他 ——

    /// 取内置的注册表服务（对应原版 `ctx.registry`）。
    ///
    /// 枚举插件运行时与 fiber 状态，诊断"谁在 PENDING、缺什么依赖"。
    ///
    /// # Errors
    ///
    /// 内置服务缺失（框架构造不完整，理论不可达）。
    pub fn registry(&self) -> Result<Arc<RegistryService>> {
        self.expect::<RegistryService>("registry")
    }

    /// 以当前 fiber 名取一个日志器（对应原版 `ctx.logger(name)` 的简化）。
    #[must_use]
    pub fn logger(&self) -> Logger {
        let name = self
            .fiber()
            .map_or_else(|_| "root".into(), |fiber| fiber.name());
        Logger::new(name)
    }
}
