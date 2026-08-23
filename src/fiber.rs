//! Fiber：一次插件挂载的运行实例与依赖驱动的生命周期状态机。
//!
//! 对应原版 `Fiber`。一个 fiber 跟踪：依赖满足情况、生命周期副作用
//! （disposables）、当前状态与启动错误。状态流转与原版一一对应：
//!
//! ```text
//!            依赖就绪                     apply 成功
//!  Pending ──────────► Loading ──────────────► Active
//!     ▲                   │ apply 失败            │ 依赖被撤销
//!     │ 依赖恢复           ▼                      ▼
//!     └────────────── Failed ◄──── (撤销已注册副作用)
//!     │                                          │
//!     └──────────── Unloading ◄──────────────────┘
//!                       │ 清理完成
//!                       ▼
//!                    Disposed（终态，来自 dispose()）
//! ```
//!
//! 关键语义（对应原版 `_reload` / `_unload` / `inertia`）：
//!
//! - **依赖驱动**：`refresh()` 在任一注入服务变化后重估，需要时驱动
//!   加载或卸载——插件作者从不手写启动顺序；
//! - **互斥转换**：同一 fiber 同时至多一个转换任务（`inertia` 槽位），
//!   任务收尾时自行清出，后续变化可再驱动；
//! - **逆序撤销**：卸载时按副作用的**逆注册顺序**执行清理；
//! - **失败即清理**：`apply` 失败的 fiber 不残留半注册的副作用。

use std::sync::{Arc, Mutex, Weak};

use tokio::task::JoinHandle;

use crate::context::{AppInner, Context};
use crate::error::{CordisError, Result};
use crate::events::event;

/// fiber 生命周期状态（对应原版 `FiberState`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FiberState {
    /// 等待注入的服务就绪。
    Pending,
    /// 插件 `apply` 正在运行。
    Loading,
    /// 已加载且其提供的服务可见。
    Active,
    /// `apply` 或其配置抛错（副作用已撤销，可被依赖变化再次拉起）。
    Failed,
    /// 副作用正在逆序执行。
    Unloading,
    /// 已被移除，终态。
    Disposed,
}

impl FiberState {
    /// 状态名（诊断输出用）。
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Loading => "loading",
            Self::Active => "active",
            Self::Failed => "failed",
            Self::Unloading => "unloading",
            Self::Disposed => "disposed",
        }
    }
}

/// `internal/status` 事件的负载。
#[derive(Debug, Clone)]
pub struct StatusEvent {
    /// 发生状态变化的 fiber。
    pub fiber: Fiber,
    /// 变化前的状态。
    pub prev: FiberState,
    /// 变化后的状态。
    pub now: FiberState,
}

/// fiber 的公开句柄（`ctx.plugin()` 的返回值）。
///
/// 句柄可克隆；持有它不会阻止 fiber 卸载。
#[derive(Clone)]
pub struct Fiber(pub(crate) Arc<FiberInner>);

impl std::fmt::Debug for Fiber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fiber")
            .field("uid", &self.0.uid())
            .field("name", &self.name())
            .field("state", &self.state())
            .finish()
    }
}

/// fiber 内部状态。
pub(crate) struct FiberInner {
    /// 应用共享状态（弱引用：App 句柄与 root fiber 才是强锚点）。
    app: Weak<AppInner>,
    /// 本 fiber 的插件（`None` = 根 fiber）。
    plugin: Option<Arc<dyn crate::registry::Plugin>>,
    /// 展示名（插件名；根为 `"root"`）。
    name: String,
    /// 声明的服务依赖名。
    inject: Vec<String>,
    /// 插件上下文（构造完成后不变）。
    ctx: OnceLockContext,
    /// 唯一 id；`None` 即已进入销毁。
    uid: Mutex<Option<u64>>,
    /// 当前生命周期状态。
    state: Mutex<FiberState>,
    /// 已注册副作用（label + 一次性清理槽），按注册顺序排列。
    disposables: Mutex<Vec<(String, crate::effect::CleanupSlotOneshot)>>,
    /// 最近一次启动失败的展示信息。
    error: Mutex<Option<String>>,
    /// 进行中的生命周期转换任务。
    inertia: Mutex<Option<JoinHandle<()>>>,
}

/// 延迟一次性赋值的插件上下文（fiber 构造与 ctx 构造互相引用）。
pub(crate) struct OnceLockContext(std::sync::OnceLock<Context>);

impl FiberInner {
    pub(crate) fn ctx(&self) -> Context {
        self.ctx
            .0
            .get()
            .expect("fiber 上下文在构造后立刻写入")
            .clone()
    }

    pub(crate) fn set_ctx(&self, ctx: Context) {
        let _ = self.ctx.0.set(ctx);
    }

    /// 当前 uid（`None` = 已销毁）。
    pub(crate) fn uid(&self) -> Option<u64> {
        *lock!(self.uid)
    }

    /// 框架内部：升级应用共享状态。
    pub(crate) fn app(&self) -> Option<Arc<AppInner>> {
        self.app.upgrade()
    }

    /// 插件本体（根 fiber 为 `None`）。
    pub(crate) fn plugin(&self) -> Option<&Arc<dyn crate::registry::Plugin>> {
        self.plugin.as_ref()
    }

    /// 展示名。
    pub(crate) fn display_name(&self) -> &str {
        &self.name
    }

    /// 当前状态（内部拷贝口径）。
    pub(crate) fn state(&self) -> FiberState {
        *lock!(self.state)
    }

    /// 是否声明注入了某个服务。
    pub(crate) fn injects(&self, name: &str) -> bool {
        self.inject.iter().any(|dep| dep == name)
    }
}

impl Fiber {
    /// 唯一 id；fiber 被销毁后为 `None`（根 fiber 恒为 `Some(0)`）。
    #[must_use]
    pub fn uid(&self) -> Option<u64> {
        self.0.uid()
    }

    /// 展示名（插件名，根为 `"root"`）。
    #[must_use]
    pub fn name(&self) -> String {
        self.0.name.clone()
    }

    /// 当前生命周期状态。
    #[must_use]
    pub fn state(&self) -> FiberState {
        *lock!(self.0.state)
    }

    /// 最近一次启动失败的错误展示（无则为 `None`）。
    #[must_use]
    pub fn error(&self) -> Option<String> {
        lock!(self.0.error).clone()
    }

    /// 已注册副作用的诊断标签列表（按注册顺序）。
    #[must_use]
    pub fn effects(&self) -> Vec<String> {
        lock!(self.0.disposables)
            .iter()
            .map(|(label, _)| label.clone())
            .collect()
    }

    /// 等待当前生命周期工作收尾，并重抛启动错误（对应原版 `fiber.await()`）。
    ///
    /// # Errors
    ///
    /// fiber 处于 `Failed` 时返回其启动错误（以消息形式恢复）。
    pub async fn ready(&self) -> Result<Fiber> {
        loop {
            let handle = lock!(self.0.inertia).take();
            match handle {
                Some(handle) => {
                    if let Err(join) = handle.await {
                        // 转换任务 panic 属于框架级 bug，如实上报
                        return Err(CordisError::PluginFailed {
                            plugin: format!("fiber {} lifecycle task", self.0.name),
                            source: Box::new(join),
                        });
                    }
                }
                None => break,
            }
        }
        if let Some(message) = self.error() {
            return Err(CordisError::PluginFailed {
                plugin: self.0.name.clone(),
                source: message.into(),
            });
        }
        Ok(self.clone())
    }

    /// 卸载本 fiber 并从注册表移除（对应原版 `fiber.dispose()`）。
    ///
    /// 异步收尾：本方法立即返回，可用 [`Self::ready`] 或
    /// [`crate::App::shutdown`] 等待清理完成。重复调用是空操作。
    pub fn dispose(&self) {
        self.0.begin_dispose();
    }

    /// 列出尚未就绪的依赖服务名（PENDING 诊断：谁在等什么）。
    ///
    /// 依赖驱动加载的另一面是静默等待——依赖无人提供时 fiber 停在
    /// `Pending` 且不报错（原版教程专设一节讲这个诊断场景）。
    #[must_use]
    pub fn missing_dependencies(&self) -> Vec<String> {
        let ctx = self.0.ctx();
        self.0
            .inject
            .iter()
            .filter(|name| ctx.resolve_service_impl(name).is_none())
            .cloned()
            .collect()
    }
}

impl FiberInner {
    /// 构造根 fiber（uid 0，直接 Active）。
    pub(crate) fn root(app: Weak<AppInner>) -> Arc<Self> {
        Arc::new(Self {
            app,
            plugin: None,
            name: "root".into(),
            inject: Vec::new(),
            ctx: OnceLockContext(std::sync::OnceLock::new()),
            uid: Mutex::new(Some(0)),
            state: Mutex::new(FiberState::Active),
            disposables: Mutex::new(Vec::new()),
            error: Mutex::new(None),
            inertia: Mutex::new(None),
        })
    }

    /// 构造插件 fiber（Pending，等待依赖）。
    ///
    /// 插件上下文由调用方随后经 [`Self::set_ctx`] 写入（fiber 与 ctx
    /// 互相引用，构造无法一步完成）。
    pub(crate) fn plugin_fiber(
        app: Weak<AppInner>,
        plugin: Arc<dyn crate::registry::Plugin>,
        name: String,
        inject: Vec<String>,
        uid: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            app,
            plugin: Some(plugin),
            name,
            inject,
            ctx: OnceLockContext(std::sync::OnceLock::new()),
            uid: Mutex::new(Some(uid)),
            state: Mutex::new(FiberState::Pending),
            disposables: Mutex::new(Vec::new()),
            error: Mutex::new(None),
            inertia: Mutex::new(None),
        })
    }

    /// 当前是否允许注册新副作用（对应原版 `assertActive`）。
    pub(crate) fn assert_active(&self) -> Result<()> {
        let state = *lock!(self.state);
        match (self.uid().is_some(), state) {
            (true, FiberState::Unloading | FiberState::Disposed) | (false, _) => {
                Err(CordisError::InactiveContext)
            }
            (true, _) => Ok(()),
        }
    }

    /// 登记一个副作用槽（`ctx.effect` 的落点）。
    pub(crate) fn push_effect(
        &self,
        label: String,
        slot: crate::effect::CleanupSlotOneshot,
    ) -> Result<()> {
        self.assert_active()?;
        lock!(self.disposables).push((label, slot));
        Ok(())
    }

    /// 写入状态并广播 `internal/status`（仅在变化时）。
    fn set_state(self: &Arc<Self>, next: FiberState) {
        let prev = {
            let mut state = lock!(self.state);
            if *state == next {
                return;
            }
            *state = next;
            next
        };
        if let Some(app) = self.app() {
            let payload = StatusEvent {
                fiber: Fiber(Arc::clone(self)),
                prev,
                now: next,
            };
            // internal/status 的观察失败只影响诊断，不影响生命周期
            let ctx = self.ctx();
            if let Err(error) = app.bus.emit(&ctx, event::STATUS, payload) {
                app.logger
                    .warn(format!("internal/status 分发失败: {error}"));
            }
        }
    }

    /// 全部注入服务是否就绪（提供方须为 Active）。
    fn deps_satisfied(&self) -> bool {
        if self.inject.is_empty() {
            return true;
        }
        let ctx = self.ctx();
        self.inject
            .iter()
            .all(|name| ctx.resolve_service_impl(name).is_some())
    }

    /// 依赖变化后重估：需要转换且当前空闲时驱动一次。
    ///
    /// 已有转换在途时什么都不做——其收尾时会重估，避免并发转换。
    pub(crate) fn refresh(self: &Arc<Self>) {
        let mut inertia = lock!(self.inertia);
        if inertia.is_some() {
            return;
        }
        let satisfied = self.deps_satisfied();
        let state = *lock!(self.state);
        let needs = match state {
            FiberState::Pending | FiberState::Failed => satisfied,
            FiberState::Active => !satisfied,
            _ => false,
        };
        if needs {
            let task = tokio::spawn(Self::run(Arc::clone(self)));
            *inertia = Some(task);
        }
    }

    /// 生命周期转换主循环：反复“重估 → 加载/卸载”，直到状态稳定。
    async fn run(self: Arc<Self>) {
        loop {
            let satisfied = self.deps_satisfied();
            let state = *lock!(self.state);
            match state {
                FiberState::Pending | FiberState::Failed if satisfied => {
                    self.set_state(FiberState::Loading);
                    match self.apply_plugin().await {
                        Ok(()) => {
                            lock!(self.error).take();
                            self.set_state(FiberState::Active);
                            self.notify_provided();
                        }
                        Err(error) => {
                            self.report_error(&error);
                            // 失败即清理：不残留半注册的副作用（对应原版
                            // _reload 失败后置 INACTIVE 触发 _unload）
                            self.set_state(FiberState::Unloading);
                            self.drain_disposables();
                            self.set_state(FiberState::Failed);
                            break;
                        }
                    }
                }
                FiberState::Active if !satisfied => {
                    self.set_state(FiberState::Unloading);
                    self.drain_disposables();
                    self.set_state(FiberState::Pending);
                }
                _ => break,
            }
        }
        // 清出自身句柄，允许下一次依赖变化驱动新转换
        lock!(self.inertia).take();
    }

    /// 运行插件本体。
    async fn apply_plugin(&self) -> Result<()> {
        let Some(plugin) = self.plugin.as_ref() else {
            return Ok(()); // 根 fiber 无插件
        };
        let ctx = self.ctx();
        plugin
            .apply(ctx)
            .await
            .map_err(|error| crate::registry::plugin_error(&self.name, error))
    }

    /// 记录并上报启动失败。
    fn report_error(&self, error: &CordisError) {
        *lock!(self.error) = Some(error.to_string());
        if let Some(app) = self.app() {
            app.logger
                .error(format!("插件 <{}> 启动失败: {error}", self.name));
        }
    }

    /// 进入 Active 后，唤醒依赖本 fiber 所提供服务的下游 fiber。
    fn notify_provided(self: &Arc<Self>) {
        let Some(app) = self.app() else { return };
        let names = app.services_provided_by(self);
        for name in names {
            // 广播走提供方上下文：携带服务作用域过滤（同作用域/global 才收到）
            let value = self.ctx().resolve_service_impl(&name);
            self.ctx().announce_service(&name, value);
            app.notify_service_change(&name);
        }
    }

    /// 逆序执行全部副作用清理（单个清理 panic 不阻断其余清理）。
    fn drain_disposables(&self) {
        let drained: Vec<(String, crate::effect::CleanupSlotOneshot)> = {
            let mut disposables = lock!(self.disposables);
            std::mem::take(&mut *disposables)
        };
        for (label, slot) in drained.into_iter().rev() {
            let cleanup = lock!(slot).take();
            if let Some(cleanup) = cleanup {
                // catch_unwind：一个副作用的清理 panic 不应吞掉其余清理
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(cleanup));
                if let Err(panic) = result {
                    let message = panic_message(&panic);
                    if let Some(app) = self.app() {
                        app.logger
                            .error(format!("副作用 <{label}> 清理 panic: {message}"));
                    }
                }
            }
        }
    }

    /// 开始销毁：标记终态、摘出注册表，并调度最终清理任务。
    pub(crate) fn begin_dispose(self: &Arc<Self>) {
        if lock!(self.uid).take().is_none() {
            return; // 已在销毁中，幂等
        }
        if let Some(app) = self.app() {
            app.unmount_fiber(self);
        }
        let Some(app) = self.app() else { return };
        let fiber = Arc::clone(self);
        let task = tokio::spawn(async move {
            fiber.dispose_all().await;
        });
        app.push_pending_disposal(task);
    }

    /// 等待在途转换收尾后执行最终清理。
    async fn dispose_all(self: Arc<Self>) {
        // 先等在途的加载/卸载结束，避免与清理竞争注册新副作用
        loop {
            let handle = lock!(self.inertia).take();
            match handle {
                Some(handle) => {
                    if let Err(ref join) = handle.await
                        && let Some(app) = self.app()
                    {
                        app.logger
                            .error(format!("fiber <{}> 转换任务 panic: {join}", self.name));
                    }
                }
                None => break,
            }
        }
        self.set_state(FiberState::Unloading);
        self.drain_disposables();
        self.set_state(FiberState::Disposed);
    }
}

/// 把 panic 载荷转成可读消息。
fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "<非字符串 panic 载荷>".into()
    }
}
