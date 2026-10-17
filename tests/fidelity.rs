//! 集成测试：对照原版源码审计后补齐的三项语义。
//!
//! - `provide` 的可用性谓词（原版 `Impl.check` / `_checkImpl`）：
//!   谓词不过 → 依赖方等待；翻转 → 服务变化唤醒后加载
//! - `set`（原版 `reflect.set`）：提供方原位覆盖，非提供方报错
//! - `internal/dispatch`（原版 dispatch 内的观测 emit）：
//!   公开事件分发前携带模式与事件名触发；框架事件豁免

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cordis::events::event;
use cordis::{App, CordisError, DispatchEvent, DispatchMode, FiberState, fn_plugin};

/// 可用性谓词为 false：注入方等待（如同服务未提供）；服务变化事件
/// （撤销→重提供）触发重估后加载。谓词翻转本身不是服务变化——原版
/// 的重估同样只由 provide/dispose 驱动。
#[tokio::test]
async fn check_predicate_gates_dependency_readiness() {
    let app = App::new().unwrap();
    let ready = Arc::new(AtomicBool::new(false));

    let flag = Arc::clone(&ready);
    let disposer = app
        .root()
        .provide_with_check("gate", 42_i32, move |value| {
            flag.load(Ordering::SeqCst) && *value > 0
        })
        .unwrap();

    let consumer = app
        .root()
        .plugin(fn_plugin("consumer", &["gate"], |_ctx| async { Ok(()) }))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // 提供方 Active 但谓词不过：依赖仍视为未就绪
    assert_eq!(consumer.state(), FiberState::Pending);

    // 服务变化（撤销 → 谓词翻转 → 重提供）是依赖重估的驱动事件
    disposer.dispose();
    ready.store(true, Ordering::SeqCst);
    app.root().provide("gate", 42_i32).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(consumer.state(), FiberState::Active);
    app.shutdown().await;
}

/// 谓词不影响读取：get/expect 照常读到值（与原版咨询点一致）。
#[tokio::test]
async fn check_predicate_does_not_affect_reads() {
    let app = App::new().unwrap();
    app.root()
        .provide_with_check("gate", 7_i32, |_| false)
        .unwrap();

    assert_eq!(app.root().get::<i32>("gate"), Some(Arc::new(7_i32)));
    assert_eq!(app.root().expect::<i32>("gate").unwrap(), Arc::new(7_i32));
    app.shutdown().await;
}

/// set：提供方原位覆盖成功，读取方立即见新值。
#[tokio::test]
async fn set_overwrites_own_service_in_place() {
    let app = App::new().unwrap();
    app.root().provide("answer", 1_i32).unwrap();

    app.root().set("answer", Arc::new(2_i32)).unwrap();
    assert_eq!(app.root().get::<i32>("answer"), Some(Arc::new(2_i32)));
    // 类型诊断跟随新值：set 更新过注册类型名
    assert!(matches!(
        app.root().expect::<u8>("answer"),
        Err(CordisError::ServiceTypeMismatch { .. })
    ));
    app.shutdown().await;
}

/// set：非提供方覆盖报 SetByNonOwner；未提供报 `ServiceNotProvided`。
#[tokio::test]
async fn set_rejects_non_owner_and_missing() {
    let app = App::new().unwrap();
    let provider = app
        .root()
        .plugin(fn_plugin("provider", &[], |ctx| async move {
            ctx.provide("answer", 1_i32)?;
            Ok(())
        }))
        .unwrap();
    provider.ready().await.unwrap();

    // 根上下文不是提供方：拒绝
    let error = app.root().set("answer", Arc::new(2_i32)).unwrap_err();
    assert!(matches!(error, CordisError::SetByNonOwner { .. }));
    // 值未被改动
    assert_eq!(app.root().get::<i32>("answer"), Some(Arc::new(1_i32)));
    // 未提供的服务名
    let error = app.root().set("missing", Arc::new(2_i32)).unwrap_err();
    assert!(matches!(error, CordisError::ServiceNotProvided(_)));
    app.shutdown().await;
}

/// internal/dispatch：公开事件分发前触发，携带模式与事件名；
/// 框架事件（internal/*）豁免。
#[tokio::test]
async fn dispatch_event_observes_public_dispatches() {
    use std::sync::Mutex;

    let app = App::new().unwrap();
    let seen: Arc<Mutex<Vec<(DispatchMode, String)>>> = Arc::new(Mutex::new(Vec::new()));

    let log = Arc::clone(&seen);
    app.root()
        .on(event::DISPATCH, move |_ctx, payload: Arc<DispatchEvent>| {
            let log = Arc::clone(&log);
            async move {
                log.lock()
                    .unwrap()
                    .push((payload.mode, payload.name.clone()));
                Ok(())
            }
        })
        .unwrap();

    app.root().emit("public-emit", 1_i32).unwrap();
    app.root().parallel("public-parallel", 1_i32).await.unwrap();
    app.root()
        .serial::<i32, i32>("public-serial", 1_i32)
        .await
        .unwrap();
    app.root()
        .waterfall("public-waterfall", 1_i32)
        .await
        .unwrap();

    // 分发观测走 emit（触发即忘）：给监听任务收尾时间
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let seen = seen.lock().unwrap().clone();
    assert!(seen.contains(&(DispatchMode::Emit, "public-emit".into())));
    assert!(seen.contains(&(DispatchMode::Parallel, "public-parallel".into())));
    assert!(seen.contains(&(DispatchMode::Serial, "public-serial".into())));
    assert!(seen.contains(&(DispatchMode::Waterfall, "public-waterfall".into())));
    // internal/* 不触发观测（含 internal/dispatch 自身，防递归）
    assert!(seen.iter().all(|(_, name)| !name.starts_with("internal/")));
    app.shutdown().await;
}
