//! 可逆副作用（Effect）与一次性清理函数（Disposer）。
//!
//! Cordis 的核心承诺是“注册是可逆的副作用”：提示词片段、工具 schema、
//! 服务、事件监听器都通过 `ctx.effect()` 安装，reload 与 teardown 时按
//! **逆注册顺序**撤销。本模块用一次性清理闭包落成同一语义：
//!
//! - [`Disposer`] 既归 fiber 所有（fiber 卸载时自动执行），
//!   也可以被持有者提前手动执行；
//! - 两条路径共享同一个 `Arc<Mutex<Option<_>>>` 槽位，先到先得，
//!   第二次执行是安全的空操作——对应原版“调用 disposer 两次是 no-op”。

use std::sync::{Arc, Mutex};

/// 单次清理动作的底层闭包。
///
/// 约定为纯同步：v1 将异步清理简化为同步（大多数清理只是注销动作），
/// 需要异步收尾的工作应在插件 `apply` 的 `Drop` 守卫或服务 `stop` 中完成。
pub type Cleanup = Box<dyn FnOnce() + Send + 'static>;

/// 一次性清理槽：`Option` 即“尚未执行”，`take()` 保证只执行一次。
pub(crate) type CleanupSlotOneshot = Arc<Mutex<Option<Cleanup>>>;

/// 注册一个副作用时返回的清理句柄。
///
/// 调用 [`Disposer::dispose`] 立即撤销副作用；不调用则由所属 fiber
/// 在卸载时逆序执行。两者竞争时只执行一次。
///
/// # 与原版的对应
///
/// 对应 Cordis `ctx.effect()` 返回的 disposer；原版允许返回
/// disposer / Promise / 生成器等多种形态，Rust 版统一收敛为单一清理闭包，
/// 生成器形态（一个 effect 产出多个 disposer）由多次调用 `ctx.effect()` 替代。
pub struct Disposer {
    label: String,
    slot: CleanupSlotOneshot,
}

impl Disposer {
    /// 由框架内部创建（用户侧应通过 `ctx.effect()` 获得）。
    pub(crate) fn new(label: impl Into<String>, slot: CleanupSlotOneshot) -> Self {
        Self {
            label: label.into(),
            slot,
        }
    }

    /// 将清理闭包装入一个新槽，返回对应的共享句柄对。
    ///
    /// 返回值依次为：fiber 侧持有的槽（卸载时消费）、
    /// 用户侧的 [`Disposer`]（提前撤销时消费）。
    pub(crate) fn from_cleanup(
        label: impl Into<String>,
        cleanup: Option<Cleanup>,
    ) -> (CleanupSlotOneshot, Self) {
        let slot: CleanupSlotOneshot = Arc::new(Mutex::new(cleanup));
        let disposer = Self::new(label, Arc::clone(&slot));
        (slot, disposer)
    }

    /// 副作用的诊断标签（如 `ctx.on("agent/request")`、`ctx.provide("tools")`）。
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// 立即执行清理；重复执行是空操作。
    pub fn dispose(self) {
        if let Some(cleanup) = self.take() {
            cleanup();
        }
    }

    /// 取出尚未执行的清理闭包；已被执行过则返回 `None`。
    fn take(&self) -> Option<Cleanup> {
        lock!(self.slot).take()
    }
}

/// RAII 守卫形态的副作用句柄：**Drop 即撤销**。
///
/// [`Disposer`] 沿用原版 JS 的语义（句柄被丢弃 ≠ 效果撤销，效果存活到
/// fiber 卸载）；[`EffectGuard`] 则是 Rust 母语的表达——守卫离开作用域
/// 时立即执行清理，与局部变量的 Drop 顺序（逆声明序）天然一致：
///
/// ```no_run
/// # use cordis::{App, Context};
/// # fn demo(ctx: &Context) -> cordis::Result<()> {
/// let a = ctx.guard("effect-a", Some(Box::new(|| println!("undo a"))))?;
/// let b = ctx.guard("effect-b", Some(Box::new(|| println!("undo b"))))?;
/// // ……作用域结束：先 undo b，后 undo a（逆声明序）
/// # let _ = (a, b);
/// # Ok(())
/// # }
/// ```
///
/// 需要回到"效果存活到 fiber 卸载"的 Cordis 语义时，用 [`Self::disarm`]
/// （解除武装、效果留给 fiber）或 [`Self::detach`]（拿回 [`Disposer`]
/// 显式管理）。两条撤销路径（guard Drop 与 fiber 卸载）共享同一一次性
/// 槽位，竞争时只执行一次。
pub struct EffectGuard {
    /// `None` 仅出现在已解除武装之后（Drop 变为空操作）。
    disposer: Option<Disposer>,
}

impl EffectGuard {
    /// 包装一个已注册的 disposer（框架内部使用）。
    pub(crate) fn wrap(disposer: Disposer) -> Self {
        Self {
            disposer: Some(disposer),
        }
    }

    /// 副作用的诊断标签。
    #[must_use]
    pub fn label(&self) -> &str {
        self.disposer.as_ref().map_or("<disarmed>", Disposer::label)
    }

    /// 解除武装：丢弃守卫但**不执行清理**，效果保留到 fiber 卸载。
    ///
    /// 对应 Cordis 插件的默认行为（`ctx.effect` 返回的 disposer 被丢弃
    /// 时效果仍然存活）。
    pub fn disarm(mut self) {
        self.disposer = None;
    }

    /// 摘下守卫、拿回显式管理的 [`Disposer`]。
    ///
    /// 返回 `None` 仅在守卫已被解除武装时出现（按值消费，正常不会发生）。
    #[must_use]
    pub fn detach(mut self) -> Option<Disposer> {
        self.disposer.take()
    }
}

impl Drop for EffectGuard {
    fn drop(&mut self) {
        if let Some(disposer) = self.disposer.take() {
            disposer.dispose();
        }
    }
}

impl std::fmt::Debug for EffectGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectGuard")
            .field("label", &self.label())
            .field("armed", &self.disposer.is_some())
            .finish()
    }
}

impl std::fmt::Debug for Disposer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 锁只用于探测状态，绝不跨 await 持有，也不在 Debug 输出中执行清理
        let executed = self.slot.lock().map_or(true, |slot| slot.is_none());
        f.debug_struct("Disposer")
            .field("label", &self.label)
            .field("executed", &executed)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 提前 dispose 后，fiber 侧槽位再消费应为空 —— 一次性语义。
    #[test]
    fn dispose_is_one_shot() {
        let counter = Arc::new(Mutex::new(0));
        let bump = {
            let counter = Arc::clone(&counter);
            let closure: Cleanup = Box::new(move || {
                *counter.lock().unwrap() += 1;
            });
            closure
        };
        let (slot, disposer) = Disposer::from_cleanup("test", Some(bump));
        disposer.dispose();
        // fiber 卸载路径再消费同一槽位：清理不应执行第二次
        assert!(slot.lock().unwrap().take().is_none());
        assert_eq!(*counter.lock().unwrap(), 1);
    }

    /// 无清理动作的副作用（仅观察）不 panic。
    #[test]
    fn none_cleanup_is_noop() {
        let (_slot, disposer) = Disposer::from_cleanup("noop", None);
        disposer.dispose();
    }
}
