//! 集成测试：`internal/get` / `internal/set` 同步拦截瀑布。
//!
//! 对应原版 Proxy trap 内的同步 waterfall（shadow / fallback / 否决）：
//! - get/expect 流经 `internal/get`：改写值、补默认、清空、错误传播
//! - provide 流经 `internal/set`：改写名/值、否决、终态键正确撤销
//! - 框架内部依赖解析不受读拦截影响（对应原版 `_checkImpl` 直查 store）
//! - 契约校验：async waterfall 不能注册拦截事件
//! - 拦截器随 fiber 卸载自动撤销；无监听器时 fast path 行为不变

use std::sync::Arc;

use cordis::events::{SyncNext, event};
use cordis::{App, CordisError, FiberState, ServiceGet, ServiceSet, fn_plugin};

/// 把擦除的服务值还原为 `i32`（测试断言用）。
fn as_i32(get: &ServiceGet) -> Option<i32> {
    get.value
        .as_ref()
        .and_then(|value| value.downcast_ref::<i32>())
        .copied()
}

// —— internal/get：shadow / fallback / 否决 ——

/// shadow：拦截者改写已提供服务的值，get 读到替换值。
#[tokio::test]
async fn get_interceptor_rewrites_value() {
    let app = App::new().unwrap();
    app.root()
        .plugin(fn_plugin("provider", &[], |ctx| async move {
            ctx.provide("answer", 42_i32)?;
            Ok(())
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    app.root()
        .on_waterfall_sync(
            event::GET,
            |_ctx, mut query: ServiceGet, next: SyncNext<ServiceGet>| {
                if query.name == "answer" && as_i32(&query) == Some(42) {
                    query.value = Some(Arc::new(7_i32));
                }
                next.call(query)
            },
        )
        .unwrap();

    assert_eq!(app.root().get::<i32>("answer"), Some(Arc::new(7_i32)));
    app.shutdown().await;
}

/// fallback：服务未提供时拦截者补默认值。
#[tokio::test]
async fn get_interceptor_fills_missing_service() {
    let app = App::new().unwrap();
    app.root()
        .on_waterfall_sync(
            event::GET,
            |_ctx, mut query: ServiceGet, next: SyncNext<ServiceGet>| {
                if query.name == "answer" && query.value.is_none() {
                    query.value = Some(Arc::new(-1_i32));
                }
                next.call(query)
            },
        )
        .unwrap();

    assert_eq!(app.root().get::<i32>("answer"), Some(Arc::new(-1_i32)));
    // 未被拦截器接管的其他服务仍为 None
    assert_eq!(app.root().get::<i32>("unrelated"), None);
    app.shutdown().await;
}

/// 否决：拦截者清空已提供服务的值，get 读不到。
#[tokio::test]
async fn get_interceptor_can_veto_read() {
    let app = App::new().unwrap();
    app.root()
        .plugin(fn_plugin("provider", &[], |ctx| async move {
            ctx.provide("answer", 42_i32)?;
            Ok(())
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    app.root()
        .on_waterfall_sync(
            event::GET,
            |_ctx, mut query: ServiceGet, next: SyncNext<ServiceGet>| {
                if query.name == "answer" {
                    query.value = None;
                }
                next.call(query)
            },
        )
        .unwrap();

    assert_eq!(app.root().get::<i32>("answer"), None);
    app.shutdown().await;
}

/// expect 有错误通道：拦截链失败直接传播；get 无通道，记日志视作不可读。
#[tokio::test]
async fn interceptor_error_propagates_through_expect() {
    let app = App::new().unwrap();
    app.root()
        .on_waterfall_sync(
            event::GET,
            |_ctx, _query: ServiceGet, _next: SyncNext<ServiceGet>| {
                Err(CordisError::ServiceNotProvided("intercepted".into()))
            },
        )
        .unwrap();

    assert!(app.root().expect::<i32>("answer").is_err());
    assert_eq!(app.root().get::<i32>("answer"), None);
    app.shutdown().await;
}

// —— internal/set：改写 / 否决 ——

/// 拦截者改写拟提供的名与值：终态才是实际写入与撤销的键值。
#[tokio::test]
async fn set_interceptor_rewrites_name_and_value() {
    let app = App::new().unwrap();
    app.root()
        .on_waterfall_sync(
            event::SET,
            |_ctx, mut set: ServiceSet, next: SyncNext<ServiceSet>| {
                if set.name == "draft" {
                    set.name = "final".into();
                    set.value = Arc::new(99_i32);
                }
                next.call(set)
            },
        )
        .unwrap();

    let disposer = app.root().provide("draft", 1_i32).unwrap();
    assert_eq!(app.root().get::<i32>("draft"), None);
    assert_eq!(app.root().get::<i32>("final"), Some(Arc::new(99_i32)));

    // disposer 撤销的是终态键：final 消失
    disposer.dispose();
    assert_eq!(app.root().get::<i32>("final"), None);
    app.shutdown().await;
}

/// 拦截者以 Err 否决写入：provide 返回错误，服务不入库。
#[tokio::test]
async fn set_interceptor_can_veto_write() {
    let app = App::new().unwrap();
    app.root()
        .on_waterfall_sync(
            event::SET,
            |_ctx, set: ServiceSet, _next: SyncNext<ServiceSet>| {
                Err(CordisError::AlreadyProvided {
                    name: set.name,
                    owner: "<intercepted>".into(),
                })
            },
        )
        .unwrap();

    assert!(app.root().provide("answer", 42_i32).is_err());
    assert_eq!(app.root().get::<i32>("answer"), None);
    app.shutdown().await;
}

// —— 结构与安全 ——

/// 拦截器内嵌套 get：监听器执行不持锁，无死锁（挂住即测试失败）。
#[tokio::test]
async fn nested_get_inside_interceptor_does_not_deadlock() {
    let app = App::new().unwrap();
    app.root().provide("inner", 5_i32).unwrap();

    app.root()
        .on_waterfall_sync(
            event::GET,
            |ctx, mut query: ServiceGet, next: SyncNext<ServiceGet>| {
                if query.name == "outer" {
                    // 嵌套读取同样流经 internal/get（快照在锁外）
                    let inner = ctx.get::<i32>("inner");
                    query.value = inner.map(|v| {
                        let base: Arc<dyn std::any::Any + Send + Sync> = Arc::new(*v + 1);
                        base
                    });
                }
                next.call(query)
            },
        )
        .unwrap();

    assert_eq!(app.root().get::<i32>("outer"), Some(Arc::new(6_i32)));
    app.shutdown().await;
}

/// async waterfall 与同步拦截是不同契约：事件已有同步契约后，
/// async 注册须得到 `EventModeMismatch`。
#[tokio::test]
async fn async_waterfall_cannot_register_on_get_event() {
    let app = App::new().unwrap();
    // 先以同步形态注册，固化 internal/get 的契约
    app.root()
        .on_waterfall_sync(
            event::GET,
            |_ctx, query: ServiceGet, next: SyncNext<ServiceGet>| next.call(query),
        )
        .unwrap();
    let error = app
        .root()
        .on_waterfall(event::GET, |_ctx, query: ServiceGet, next| async move {
            next.call(query).await
        })
        .unwrap_err();
    assert!(matches!(error, CordisError::EventModeMismatch { .. }));
    app.shutdown().await;
}

/// 拦截器登记为注册方 fiber 的副作用：fiber 卸载即失效。
#[tokio::test]
async fn interceptor_unloads_with_fiber() {
    let app = App::new().unwrap();
    app.root().provide("answer", 42_i32).unwrap();

    let guard = app
        .root()
        .plugin(fn_plugin("interceptor", &[], |ctx| async move {
            ctx.on_waterfall_sync(
                event::GET,
                |_ctx, mut query: ServiceGet, next: SyncNext<ServiceGet>| {
                    if query.name == "answer" {
                        query.value = Some(Arc::new(7_i32));
                    }
                    next.call(query)
                },
            )?;
            Ok(())
        }))
        .unwrap();
    guard.ready().await.unwrap();
    assert_eq!(app.root().get::<i32>("answer"), Some(Arc::new(7_i32)));

    guard.dispose();
    // 卸载在后台任务中收尾，等副作用（拦截器移除）执行完
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(app.root().get::<i32>("answer"), Some(Arc::new(42_i32)));
    app.shutdown().await;
}

/// 框架依赖解析绕过读拦截（对应原版 _checkImpl 直查 store）：
/// 拦截者补出的服务不能让注入它的 fiber 启动。
#[tokio::test]
async fn dependency_resolution_bypasses_read_interception() {
    let app = App::new().unwrap();
    app.root()
        .on_waterfall_sync(
            event::GET,
            |_ctx, mut query: ServiceGet, next: SyncNext<ServiceGet>| {
                if query.name == "missing" {
                    query.value = Some(Arc::new(1_i32));
                }
                next.call(query)
            },
        )
        .unwrap();

    let consumer = app
        .root()
        .plugin(fn_plugin("consumer", &["missing"], |_ctx| async { Ok(()) }))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(consumer.state(), FiberState::Pending);
    app.shutdown().await;
}

/// 多层拦截按注册顺序执行（先注册者外层）；prepend 可抢到最外层。
#[tokio::test]
async fn layered_interceptors_run_in_registration_order() {
    let app = App::new().unwrap();
    app.root()
        .plugin(fn_plugin("provider", &[], |ctx| async move {
            ctx.provide("answer", 1_i32)?;
            Ok(())
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    // 外层：+10
    app.root()
        .on_waterfall_sync(
            event::GET,
            |_ctx, mut query: ServiceGet, next: SyncNext<ServiceGet>| {
                if query.name == "answer" {
                    query.value = Some(Arc::new(as_i32(&query).unwrap() + 10));
                }
                next.call(query)
            },
        )
        .unwrap();
    // 内层：+100
    app.root()
        .on_waterfall_sync(
            event::GET,
            |_ctx, mut query: ServiceGet, next: SyncNext<ServiceGet>| {
                if query.name == "answer" {
                    query.value = Some(Arc::new(as_i32(&query).unwrap() + 100));
                }
                next.call(query)
            },
        )
        .unwrap();

    assert_eq!(app.root().get::<i32>("answer"), Some(Arc::new(111_i32)));

    // prepend 抢到最外层：先执行 +1000
    app.root()
        .on_waterfall_sync_opt(
            event::GET,
            |_ctx, mut query: ServiceGet, next: SyncNext<ServiceGet>| {
                if query.name == "answer" {
                    query.value = Some(Arc::new(as_i32(&query).unwrap() + 1000));
                }
                next.call(query)
            },
            cordis::ListenOptions {
                prepend: true,
                global: false,
            },
        )
        .unwrap();
    assert_eq!(app.root().get::<i32>("answer"), Some(Arc::new(1111_i32)));
    app.shutdown().await;
}

/// 无拦截器时 fast path 行为不变：get/expect 与裸解析一致。
#[tokio::test]
async fn fast_path_unchanged_without_listeners() {
    let app = App::new().unwrap();
    app.root().provide("answer", 42_i32).unwrap();

    assert_eq!(app.root().get::<i32>("answer"), Some(Arc::new(42_i32)));
    assert_eq!(app.root().get::<u8>("answer"), None); // 类型不符静默 None
    assert_eq!(app.root().get::<i32>("missing"), None);
    let error = app.root().expect::<u8>("answer").unwrap_err();
    assert!(matches!(error, CordisError::ServiceTypeMismatch { .. }));
    app.shutdown().await;
}
