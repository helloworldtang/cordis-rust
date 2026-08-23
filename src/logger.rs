//! 内置日志服务。
//!
//! 对应原版 `ctx.logger`（LoggerService）。v1 刻意保持零依赖：只输出到
//! stderr、以 epoch 秒数近似时间戳；需要结构化日志的用户可以自己提供
//! 一个同名 `logger` 服务替换默认实现——这正是“一切皆可替换”的框架语义。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// 日志级别。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// 常规运行信息。
    Info,
    /// 需要关注但不影响运行。
    Warn,
    /// 错误（含被吞掉的卸载错误）。
    Error,
}

impl Level {
    /// 级别的固定展示文本。
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

/// 具名日志器：`[ts LEVEL name] message` 单行输出。
///
/// 框架内部用它报告插件启动失败、卸载错误等“不允许向上传播”的噪声；
/// 插件通过 [`crate::Context::logger`] 取到自己名字的实例。
#[derive(Debug, Clone)]
pub struct Logger {
    name: Arc<str>,
}

impl Logger {
    /// 以给定作用域名创建日志器。
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Arc::from(name.into().as_str()),
        }
    }

    /// 日志器名字。
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 输出一条日志（stderr，立刻刷新）。
    fn log(&self, level: Level, message: impl std::fmt::Display) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        eprintln!("[{ts} {} {}] {message}", level.as_str(), self.name);
    }

    /// 输出 INFO 级日志。
    pub fn info(&self, message: impl std::fmt::Display) {
        self.log(Level::Info, message);
    }

    /// 输出 WARN 级日志。
    pub fn warn(&self, message: impl std::fmt::Display) {
        self.log(Level::Warn, message);
    }

    /// 输出 ERROR 级日志。
    pub fn error(&self, message: impl std::fmt::Display) {
        self.log(Level::Error, message);
    }
}
