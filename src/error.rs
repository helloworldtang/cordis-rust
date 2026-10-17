//! 框架错误类型。
//!
//! Cordis 用 `CordisError(code)` 携带稳定的机器可读错误码；Rust 版本用
//! `thiserror` 将同一组错误码落成类型层级，供插件与框架内部统一传播。

use std::any::TypeId;

/// Cordis 框架错误。
///
/// 每个变体对应一个稳定的使用契约违反点（对应原版 `CordisError.Code`），
/// 插件可以通过 `matches!` 精确分支处理，而不必解析错误消息。
#[derive(Debug, thiserror::Error)]
pub enum CordisError {
    /// 在已销毁（或正在卸载）的上下文上注册副作用。
    ///
    /// 对应原版 `INACTIVE_EFFECT`：fiber 的 uid 已被清空，或状态为
    /// `Unloading` 时，`ctx.effect()` / `ctx.on()` / `ctx.provide()` 都会拒绝。
    #[error("cannot create effect on inactive context")]
    InactiveContext,

    /// 传入的对象不是合法插件形态。
    ///
    /// 对应 registry 的 `resolve()` 失败。Rust 版本中插件必须实现
    /// [`crate::Plugin`] trait 并满足 `Send + Sync + 'static`。
    #[error("invalid plugin: {0}")]
    InvalidPlugin(String),

    /// 插件挂载（`apply` 阶段）失败。
    #[error("plugin {plugin:?} failed to start: {source}")]
    PluginFailed {
        /// 失败插件的展示名。
        plugin: String,
        /// 插件返回的错误。
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// 读取一个从未提供过的服务名。
    ///
    /// 服务查找只沿作用域链进行，名字不存在时返回本错误而不是 panic，
    /// 以便调用方回退到默认实现。
    #[error("service {0:?} is not provided")]
    ServiceNotProvided(String),

    /// 服务名存在，但实际类型与请求的类型不符。
    ///
    /// 服务以名字为键、以 `dyn Any` 存储，`get::<T>(name)` 会在向下转型
    /// 失败时报告本错误，提示注册与消费两端类型漂移。
    #[error("service {name:?} is registered as {actual}, but requested as {expected}")]
    ServiceTypeMismatch {
        /// 服务名。
        name: String,
        /// 注册时的负载类型描述。
        actual: &'static str,
        /// 请求方期望的类型描述。
        expected: &'static str,
    },

    /// 服务名在当前隔离作用域内已被其他 fiber 提供（对应原版重复注册报错）。
    #[error("service {name:?} has already been provided at <{owner}>")]
    AlreadyProvided {
        /// 服务名。
        name: String,
        /// 先占的提供方 fiber 名。
        owner: String,
    },

    /// 覆盖了非当前 fiber 提供的服务（对应原版 `reflect.set` 的
    /// "in multiple fibers" 报错）：只有提供方能原位改写自己的服务值。
    #[error("cannot set service {name:?} provided at <{owner}>")]
    SetByNonOwner {
        /// 服务名。
        name: String,
        /// 实际提供方 fiber 名。
        owner: String,
    },

    /// 事件的使用方式与其声明的分发模式不符。
    ///
    /// 每个事件名只允许一种分发模式（`emit` / `waterfall` / `parallel` /
    /// `serial`），这是事件的公开契约；先用一种模式注册、再用另一种模式
    /// 分发（或注册）会得到本错误。
    #[error("event {event:?} is dispatched as {expected}, but registered as {actual}")]
    EventModeMismatch {
        /// 事件名。
        event: String,
        /// 首次注册时确定的模式。
        actual: &'static str,
        /// 本次调用使用的模式。
        expected: &'static str,
    },

    /// 同一事件名在不同调用点的负载类型不一致。
    #[error(
        "event {event:?} payload type mismatch: registered as {actual:?}, dispatched as {expected:?}"
    )]
    EventPayloadMismatch {
        /// 事件名。
        event: String,
        /// 首次注册时记录的负载类型。
        actual: TypeId,
        /// 本次分发使用的负载类型。
        expected: TypeId,
    },

    /// `parallel` 分发中一个或多个监听器失败。
    #[error("{count} listener(s) failed in parallel dispatch of {event:?}")]
    ParallelDispatchFailed {
        /// 事件名。
        event: String,
        /// 失败监听器数量。
        count: usize,
        /// 逐个监听器的错误，顺序与注册顺序一致。
        sources: Vec<CordisError>,
    },
}

impl CordisError {
    /// 返回稳定错误码（变体名），对应原版 `CordisError.code` 字段。
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::InactiveContext => "INACTIVE_CONTEXT",
            Self::InvalidPlugin(_) => "INVALID_PLUGIN",
            Self::PluginFailed { .. } => "PLUGIN_FAILED",
            Self::ServiceNotProvided(_) => "SERVICE_NOT_PROVIDED",
            Self::ServiceTypeMismatch { .. } => "SERVICE_TYPE_MISMATCH",
            Self::AlreadyProvided { .. } => "ALREADY_PROVIDED",
            Self::SetByNonOwner { .. } => "SET_BY_NON_OWNER",
            Self::EventModeMismatch { .. } => "EVENT_MODE_MISMATCH",
            Self::EventPayloadMismatch { .. } => "EVENT_PAYLOAD_MISMATCH",
            Self::ParallelDispatchFailed { .. } => "PARALLEL_DISPATCH_FAILED",
        }
    }
}

/// 框架内部使用的 `Result` 别名。
pub type Result<T, E = CordisError> = std::result::Result<T, E>;
