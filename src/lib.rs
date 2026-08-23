//! # Cordis（Rust 实现）
//!
//! Cordis 是一个**插件框架**：插件向共享上下文贡献服务、类型化事件与
//! 可逆副作用。本 crate 是 deepseek-harness 底层 vendor 的 Cordis
//! （TypeScript）核心语义的 Rust 移植，五个核心概念一一对应：
//!
//! 1. **插件是实现服务的对象** —— [`Plugin`] trait / [`Service`] trait；
//! 2. **上下文是服务的容器** —— [`Context`]，服务占据稳定的名字
//!    （如 `"tools"`、`"llm"`），按 key 查找而非导入实现；
//! 3. **通过 `inject` 声明依赖** —— 插件等待所需服务就绪才启动，
//!    加载顺序由依赖表达，而非手动编排；
//! 4. **类型化事件用于通信** —— [`EventBus`] 的 `emit` / `waterfall` /
//!    `parallel` / `serial` 四种分发模式；
//! 5. **注册是可逆的副作用** —— [`Context::effect`] / [`Disposer`] 与
//!    RAII 形态的 [`Context::guard`] / [`EffectGuard`]，
//!    reload 与 teardown 时逆序撤销。
//!
//! ## 快速上手
//!
//! ```
//! use cordis::{fn_plugin, prelude::*};
//!
//! #[tokio::main]
//! async fn main() -> cordis::Result<()> {
//!     let app = App::new()?;
//!     let root = app.root();
//!
//!     // 提供者：贡献 "database" 服务
//!     root.plugin(service_plugin(Database::new("sqlite:memory")))?;
//!
//!     // 消费者：声明依赖，框架保证它只在 database 就绪后启动
//!     root.plugin(fn_plugin("migrator", &["database"], |ctx: Context| async move {
//!         // get::<T> / expect::<T> 始终返回 Arc<T>
//!         let db = ctx.expect::<Database>("database")?;
//!         db.migrate().await
//!     }))?;
//!
//!     app.shutdown().await;
//!     Ok(())
//! }
//! # struct Database(&'static str);
//! # impl Database { fn new(s: &'static str) -> Self { Self(s) } async fn migrate(&self) -> cordis::Result<()> { Ok(()) } }
//! # impl cordis::Service for Database { fn name(&self) -> &str { "database" } }
//! ```
//!
//! 详细的架构解读（结构 / 角色 / 关系 / 交互规范）见 README；
//! 框架本质与移植取舍见 `docs/cordis-essence.md`；遗留问题见 `TODO.md`。
//!
//! ## 运行时要求
//!
//! 框架的插件加载与事件分发依赖 tokio 运行时：所有入口（`App::new`
//! 之后的挂载/分发）须在 runtime 上下文中调用。

#![deny(missing_docs)]

/// 加锁并容忍中毒。
///
/// 框架的所有清理动作都被 `catch_unwind` 保护，持锁 panic 后锁数据仍
/// 处于可用状态；选择恢复而不是传播 panic，使框架本身没有 panic 路径
/// （对应原版"卸载错误记日志、不中断其余清理"的行为）。
macro_rules! lock {
    ($mutex:expr) => {
        $mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    };
}

pub mod context;
pub mod effect;
pub mod error;
pub mod events;
pub mod fiber;
pub mod logger;
pub mod registry;
pub mod service;

pub use context::{App, Context, RegistryService, ServiceEvent};
pub use effect::{Cleanup, Disposer, EffectGuard};
pub use error::{CordisError, Result};
pub use events::{BoxFuture, DispatchMode, EventBus, Next, event};
pub use fiber::{Fiber, FiberState, StatusEvent};
pub use logger::{Level, Logger};
pub use registry::{FnPlugin, Plugin, fn_plugin};
pub use service::{Service, ServicePlugin, service_plugin, service_plugin_arc};

/// 隔离作用域键（服务存储与事件作用域过滤共用）。
pub(crate) type ScopeKey = u64;

/// 根作用域：未声明 isolate 的服务名/事件监听都在此解析。
pub(crate) const ROOT_SCOPE: ScopeKey = 0;

/// 监听器注册选项（对应原版 `EventOptions`）。
#[derive(Debug, Clone, Copy, Default)]
pub struct ListenOptions {
    /// 让监听器先于既有监听器执行。
    ///
    /// waterfall 事件中即"更外层"；仅当监听器必须在普通注册之前运行时
    /// 才使用（对应原版文档对 `prepend: true` 的告诫）。
    pub prepend: bool,
    /// 豁免作用域过滤：无论派发方携带何种过滤条件都接收事件。
    ///
    /// 对应原版 `EventOptions.global`。作用域过滤目前作用于
    /// `internal/service`（服务变更只广播给同作用域的监听器）；
    /// 普通业务事件的分发不过滤。
    pub global: bool,
}

/// 常用类型的预导入。
pub mod prelude {
    pub use crate::Disposer;
    pub use crate::EffectGuard;
    pub use crate::context::{App, Context};
    pub use crate::error::{CordisError, Result};
    pub use crate::events::Next;
    pub use crate::fiber::{Fiber, FiberState};
    pub use crate::registry::{Plugin, fn_plugin};
    pub use crate::service::{Service, service_plugin};
}
