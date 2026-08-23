//! Service trait：既是插件又是服务实例的挂载模式。
//!
//! 对应原版 `Service` 基类：子类构造时 `super(ctx, name)` 即把自己注册为
//! `ctx.<name>`。Rust 版把这一模式拆成两半——**服务本体**实现 [`Service`]
//! trait（名字 + start/stop 钩子），**挂载**交给 [`service_plugin`] 产生的
//! 适配插件：
//!
//! 1. `apply` 先把 `Arc<S>` 提供到 `ctx`（Loading 期间注册，依赖方要等
//!    fiber 进入 `Active` 才能读到——与原版一致）；
//! 2. 再调用 [`Service::start`] 完成需要上下文的初始化；
//! 3. 卸载时 [`Service::stop`] 随副作用的逆序执行被调用。
//!
//! 这样，"插件是实现 Service 的对象"在 Rust 侧就落成"插件挂载一个
//! Service"。

use std::sync::Arc;

use crate::context::Context;
use crate::error::Result;
use crate::events::BoxFuture;
use crate::registry::Plugin;

/// 服务本体：一个具名的、可启停的能力单元。
///
/// 实现者通常是"能力域"的入口对象（如工具注册表、模型总线），消费方
/// 通过 `ctx.get::<Self>(name)`（得到 `Arc<Self>`）或
/// [`crate::Plugin::inject`] 取用。
pub trait Service: Send + Sync + 'static {
    /// 服务名（挂载后占据 `ctx` 上的这个 key）。
    fn name(&self) -> &str;

    /// 服务启动钩子：在服务已注册到上下文之后、fiber 进入 `Active`
    /// 之前调用。默认无操作。
    ///
    /// 返回 `Err` 会使挂载失败（服务注册随之撤销）。
    fn start(&self, _ctx: Context) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    /// 服务停止钩子：fiber 卸载、服务撤销之前调用。默认无操作。
    fn stop(&self) {}
}

/// [`Service`] 的插件适配器（由 [`service_plugin`] 构造）。
pub struct ServicePlugin<S: Service> {
    service: Arc<S>,
}

impl<S: Service> ServicePlugin<S> {
    /// 服务实例的共享句柄（挂载前即可持有，便于外部直连）。
    #[must_use]
    pub fn service(&self) -> Arc<S> {
        Arc::clone(&self.service)
    }
}

impl<S: Service> Plugin for ServicePlugin<S> {
    fn name(&self) -> &str {
        self.service.name()
    }

    fn apply(&self, ctx: Context) -> BoxFuture<'static, Result<()>> {
        let service = Arc::clone(&self.service);
        Box::pin(async move {
            // 先注册再启动：start 里已经可以 ctx.expect::<Arc<Self>>(name)
            ctx.provide_arc(service.name(), Arc::clone(&service))?;
            service.start(ctx).await
        })
    }
}

/// 把一个 [`Service`] 实例包装成插件（对应原版 `ctx.plugin(ServiceSubclass)`）。
///
/// # Examples
///
/// ```
/// use cordis::{service_plugin, Plugin, Service};
/// use std::sync::atomic::AtomicUsize;
///
/// struct Counter(AtomicUsize);
///
/// impl Service for Counter {
///     fn name(&self) -> &str { "counter" }
/// }
///
/// let plugin = service_plugin(Counter(AtomicUsize::new(0)));
/// assert_eq!(plugin.name(), "counter");
/// ```
#[must_use]
pub fn service_plugin<S: Service>(service: S) -> ServicePlugin<S> {
    ServicePlugin {
        service: Arc::new(service),
    }
}

/// [`Service`] 的共享句柄包装版本（服务实例已由多方持有时使用）。
#[must_use]
pub fn service_plugin_arc<S: Service>(service: Arc<S>) -> ServicePlugin<S> {
    ServicePlugin { service }
}
