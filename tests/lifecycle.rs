//! 集成测试：核心语义契约的行为验证。
//!
//! 每个测试对应 README"交互规范"中的一条承诺：
//! - 依赖编排（inject 等待、顺序保证）
//! - 依赖撤销 → 自动卸载 → 依赖恢复 → 自动重启
//! - 四种分发模式的语义（emit/waterfall/parallel/serial）
//! - 事件契约校验（模式/负载类型冲突报错）
//! - 副作用的逆序撤销与一次性
//! - isolate 隔离作用域
//! - 启动失败的 Failed 状态与副作用回滚

// trait 实现里的 `fn name(&self) -> &str` 返回字面量会触发
// unnecessary_literal_bound，但实现签名必须与 trait 一致，无法采纳建议
#![allow(clippy::unnecessary_literal_bound)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cordis::events::{Next, event};
use cordis::{App, CordisError, FiberState, Service, fn_plugin, service_plugin};

/// 收集事件发生顺序的共享日志。
type Journal = Arc<Mutex<Vec<&'static str>>>;

/// 往日志追加一条记录。
fn log(journal: &Journal, entry: &'static str) {
    journal.lock().unwrap().push(entry);
}

/// 简单计数服务。
struct Counter(AtomicUsize);

impl Service for Counter {
    fn name(&self) -> &str {
        "counter"
    }
}

// —— 依赖编排 ——

/// 消费者插件必须等提供者 Active 后才运行：journal 中 provider-start
/// 先于 consumer-start。
#[tokio::test]
async fn inject_orders_startup_by_dependency() {
    let app = App::new().unwrap();
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));

    let consumer_log = Arc::clone(&journal);
    app.root()
        .plugin(fn_plugin("consumer", &["counter"], move |ctx| {
            let journal = Arc::clone(&consumer_log);
            async move {
                // inject 已保证就绪；expect 不应失败
                let counter = ctx.expect::<Counter>("counter").unwrap();
                assert_eq!(counter.0.load(Ordering::SeqCst), 42);
                log(&journal, "consumer-start");
                Ok(())
            }
        }))
        .unwrap();

    let provider_log = Arc::clone(&journal);
    app.root()
        .plugin(fn_plugin("provider", &[], move |ctx| {
            let journal = Arc::clone(&provider_log);
            async move {
                ctx.provide("counter", Counter(AtomicUsize::new(42)))?;
                log(&journal, "provider-start");
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    // 给 consumer 的加载任务收尾时间
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        *journal.lock().unwrap(),
        vec!["provider-start", "consumer-start"]
    );
    app.shutdown().await;
}

/// 依赖撤销后，消费者自动卸载（副作用逆序执行）并回到 Pending。
#[tokio::test]
async fn dependent_unloads_when_service_goes_away() {
    let app = App::new().unwrap();
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));

    let consumer_log = Arc::clone(&journal);
    let consumer = app
        .root()
        .plugin(fn_plugin("consumer", &["counter"], move |ctx| {
            let journal = Arc::clone(&consumer_log);
            async move {
                let counter = ctx.expect::<Counter>("counter").unwrap();
                assert_eq!(counter.0.load(Ordering::SeqCst), 42);
                let cleanup_journal = Arc::clone(&journal);
                ctx.effect(
                    "side-effect",
                    Some(Box::new(move || {
                        cleanup_journal.lock().unwrap().push("consumer-cleanup");
                    })),
                )?;
                log(&journal, "consumer-start");
                Ok(())
            }
        }))
        .unwrap();

    let provider = app
        .root()
        .plugin(fn_plugin("provider", &[], |ctx| async move {
            ctx.provide("counter", Counter(AtomicUsize::new(42)))?;
            Ok(())
        }))
        .unwrap();
    provider.ready().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(consumer.state(), FiberState::Active);

    // 撤销提供方 → 消费者依赖消失 → 自动卸载
    provider.dispose();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(consumer.state(), FiberState::Pending);
    assert!(journal.lock().unwrap().contains(&"consumer-cleanup"));

    app.shutdown().await;
}

/// 同名服务重复提供会被拒绝（对应原版重复注册报错）。
#[tokio::test]
async fn duplicate_provide_is_rejected() {
    let app = App::new().unwrap();
    app.root()
        .plugin(fn_plugin("provider", &[], |ctx| async move {
            ctx.provide("counter", Counter(AtomicUsize::new(1)))?;
            // 同一 fiber 内再次提供同名服务
            match ctx.provide("counter", Counter(AtomicUsize::new(2))) {
                Err(CordisError::AlreadyProvided { name, owner }) => {
                    assert_eq!(name, "counter");
                    assert_eq!(owner, "provider");
                    Ok(())
                }
                other => panic!("期望 AlreadyProvided，实际 {other:?}"),
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();
    app.shutdown().await;
}

/// 服务类型不符时给出带注册类型名的精确错误。
#[tokio::test]
async fn service_type_mismatch_reports_types() {
    let app = App::new().unwrap();
    app.root()
        .plugin(fn_plugin("provider", &[], |ctx| async move {
            ctx.provide("counter", Counter(AtomicUsize::new(1)))?;
            match ctx.expect::<String>("counter") {
                Err(CordisError::ServiceTypeMismatch {
                    name,
                    actual,
                    expected,
                }) => {
                    assert_eq!(name, "counter");
                    assert!(actual.contains("Counter"), "actual={actual}");
                    assert!(expected.contains("String"), "expected={expected}");
                    Ok(())
                }
                other => panic!("期望 ServiceTypeMismatch，实际 {other:?}"),
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();
    app.shutdown().await;
}

// —— 生命周期 ——

/// apply 失败 → Failed 状态 + 已注册副作用被撤销。
#[tokio::test]
async fn failed_plugin_rolls_back_effects() {
    let app = App::new().unwrap();
    let cleaned = Arc::new(AtomicUsize::new(0));

    let bump = Arc::clone(&cleaned);
    let fiber = app
        .root()
        .plugin(fn_plugin("boom", &[], move |ctx| {
            let bump = Arc::clone(&bump);
            async move {
                ctx.effect(
                    "doomed",
                    Some(Box::new(move || {
                        bump.fetch_add(1, Ordering::SeqCst);
                    })),
                )?;
                Err(CordisError::InvalidPlugin("deliberate".into()))
            }
        }))
        .unwrap();

    let result = fiber.ready().await;
    assert!(result.is_err(), "ready 应重抛启动错误");
    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(
        cleaned.load(Ordering::SeqCst),
        1,
        "失败前注册的副作用应被撤销"
    );
    app.shutdown().await;
}

/// teardown 顺序与挂载顺序相反（父的逆序副作用表保证）。
#[tokio::test]
async fn teardown_runs_in_reverse_mount_order() {
    let app = App::new().unwrap();
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));

    for name in ["first", "second", "third"] {
        let log = Arc::clone(&journal);
        app.root()
            .plugin(fn_plugin(name, &[], move |ctx| {
                let log = Arc::clone(&log);
                let cleanup_name: &'static str = match name {
                    "first" => "cleanup-first",
                    "second" => "cleanup-second",
                    _ => "cleanup-third",
                };
                async move {
                    ctx.effect(
                        "mount",
                        Some(Box::new(move || log.lock().unwrap().push(cleanup_name))),
                    )?;
                    Ok(())
                }
            }))
            .unwrap()
            .ready()
            .await
            .unwrap();
    }

    app.shutdown().await;
    assert_eq!(
        *journal.lock().unwrap(),
        vec!["cleanup-third", "cleanup-second", "cleanup-first"]
    );
}

/// internal/status 可观察生命周期变化。
#[tokio::test]
async fn internal_status_event_observable() {
    let app = App::new().unwrap();
    let states = Arc::new(Mutex::new(Vec::new()));

    let record = Arc::clone(&states);
    app.root()
        .plugin(fn_plugin("watcher", &[], move |ctx| {
            let record = Arc::clone(&record);
            async move {
                ctx.on::<cordis::StatusEvent, _, _>(
                    event::STATUS,
                    move |_ctx, status: Arc<cordis::StatusEvent>| {
                        let record = Arc::clone(&record);
                        async move {
                            record.lock().unwrap().push(status.now);
                            Ok(())
                        }
                    },
                )?;
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    // 挂载一个目标插件，制造 Pending → Loading → Active 的状态流
    app.root()
        .plugin(fn_plugin("target", &[], |_ctx| async { Ok(()) }))
        .unwrap()
        .ready()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let observed = states.lock().unwrap().clone();
    assert!(
        observed.contains(&FiberState::Active),
        "至少观察到 Active，实际 {observed:?}"
    );
    app.shutdown().await;
}

// —— 事件契约 ——

/// waterfall：先注册者在最外层，改写值后经 next 委托。
#[tokio::test]
async fn waterfall_composes_listeners_in_order() {
    let app = App::new().unwrap();
    app.root()
        .plugin(fn_plugin("layers", &[], |ctx| async move {
            ctx.on_waterfall::<Vec<String>, _, _>(
                "chain",
                |_ctx, mut value, next: Next<Vec<String>>| async move {
                    value.push("outer".into());
                    next.call(value).await
                },
            )?;
            ctx.on_waterfall::<Vec<String>, _, _>(
                "chain",
                |_ctx, mut value, next: Next<Vec<String>>| async move {
                    value.push("inner".into());
                    next.call(value).await
                },
            )?;
            Ok(())
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    let result: Vec<String> = app.root().waterfall("chain", Vec::new()).await.unwrap();
    assert_eq!(result, vec!["outer", "inner"]);
    app.shutdown().await;
}

/// waterfall：不调用 next 即短路，下游与链尾默认行为不执行。
#[tokio::test]
async fn waterfall_short_circuits_without_next() {
    let app = App::new().unwrap();
    let inner_ran = Arc::new(AtomicUsize::new(0));

    let ran = Arc::clone(&inner_ran);
    app.root()
        .plugin(fn_plugin("veto", &[], move |ctx| {
            let ran = Arc::clone(&ran);
            async move {
                // 不调 next：单决策事件的否决
                ctx.on_waterfall::<u32, _, _>(
                    "decide",
                    |_ctx, _value, _next: Next<u32>| async move { Ok(99) },
                )?;
                ctx.on_waterfall::<u32, _, _>("decide", move |_ctx, _value, _next: Next<u32>| {
                    let ran = Arc::clone(&ran);
                    async move {
                        ran.fetch_add(1, Ordering::SeqCst);
                        Ok(0)
                    }
                })?;
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    let decision = app.root().waterfall("decide", 1u32).await.unwrap();
    assert_eq!(decision, 99);
    assert_eq!(inner_ran.load(Ordering::SeqCst), 0, "被短路后下游不应执行");
    app.shutdown().await;
}

/// serial：首个 Some 决策短路，其余监听器不再执行。
#[tokio::test]
async fn serial_bails_on_first_decision() {
    let app = App::new().unwrap();
    let later_ran = Arc::new(AtomicUsize::new(0));

    let ran = Arc::clone(&later_ran);
    app.root()
        .plugin(fn_plugin("policies", &[], move |ctx| {
            let ran = Arc::clone(&ran);
            async move {
                ctx.on_serial::<u32, &'static str, _, _>(
                    "policy",
                    |_ctx, value: Arc<u32>| async move { (*value >= 10).then_some("big") },
                )?;
                ctx.on_serial::<u32, &'static str, _, _>(
                    "policy",
                    move |_ctx, _value: Arc<u32>| {
                        let ran = Arc::clone(&ran);
                        async move {
                            ran.fetch_add(1, Ordering::SeqCst);
                            Some("fallback")
                        }
                    },
                )?;
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    let decision: Option<&'static str> = app.root().serial("policy", 42u32).await.unwrap();
    assert_eq!(decision, Some("big"));
    assert_eq!(later_ran.load(Ordering::SeqCst), 0, "首个 Some 后应短路");

    let none: Option<&'static str> = app.root().serial("policy", 1u32).await.unwrap();
    assert_eq!(none, Some("fallback"));
    app.shutdown().await;
}

/// parallel：所有监听器都执行，失败被聚合上报。
#[tokio::test]
async fn parallel_runs_all_and_aggregates_errors() {
    let app = App::new().unwrap();
    let ran = Arc::new(AtomicUsize::new(0));

    let ran2 = Arc::clone(&ran);
    app.root()
        .plugin(fn_plugin("observers", &[], move |ctx| {
            let ran = Arc::clone(&ran2);
            async move {
                ctx.on::<u32, _, _>("fanout", |_ctx, _v: Arc<u32>| async { Ok(()) })?;
                ctx.on::<u32, _, _>("fanout", move |_ctx, _v: Arc<u32>| {
                    let ran = Arc::clone(&ran);
                    async move {
                        ran.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })?;
                ctx.on::<u32, _, _>("fanout", |_ctx, _v: Arc<u32>| async {
                    Err(CordisError::InvalidPlugin("listener boom".into()))
                })?;
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    match app.root().parallel("fanout", 1u32).await {
        Err(CordisError::ParallelDispatchFailed { count, sources, .. }) => {
            assert_eq!(count, 1);
            assert_eq!(sources.len(), 1);
        }
        other => panic!("期望 ParallelDispatchFailed，实际 {other:?}"),
    }
    assert_eq!(ran.load(Ordering::SeqCst), 1, "其他监听器照常执行");
    app.shutdown().await;
}

/// 同一事件名混用两种模式会被拒绝（契约固化）。
#[tokio::test]
async fn event_mode_contract_is_enforced() {
    let app = App::new().unwrap();
    app.root()
        .plugin(fn_plugin("mixed", &[], |ctx| async move {
            ctx.on::<u32, _, _>("oops", |_ctx, _v: Arc<u32>| async { Ok(()) })?;
            // 同名事件再用 waterfall 注册：形态冲突
            let conflict = ctx
                .on_waterfall::<u32, _, _>("oops", |_ctx, v, next: Next<u32>| async move {
                    next.call(v).await
                });
            match conflict {
                Err(CordisError::EventModeMismatch {
                    actual, expected, ..
                }) => {
                    assert_eq!((actual, expected), ("emit|parallel", "waterfall"));
                    Ok(())
                }
                other => panic!("期望 EventModeMismatch，实际 {other:?}"),
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();
    app.shutdown().await;
}

/// 负载类型漂移在注册时即被拒绝。
#[tokio::test]
async fn payload_type_contract_is_enforced() {
    let app = App::new().unwrap();
    app.root()
        .plugin(fn_plugin("typed", &[], |ctx| async move {
            ctx.on::<u32, _, _>("value", |_ctx, _v: Arc<u32>| async { Ok(()) })?;
            match ctx.on::<String, _, _>("value", |_ctx, _v: Arc<String>| async { Ok(()) }) {
                Err(CordisError::EventPayloadMismatch { .. }) => Ok(()),
                other => panic!("期望 EventPayloadMismatch，实际 {other:?}"),
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();
    app.shutdown().await;
}

/// once：监听器首次触发后自移除。
#[tokio::test]
async fn once_listener_fires_single_time() {
    let app = App::new().unwrap();
    let fired = Arc::new(AtomicUsize::new(0));

    let fired2 = Arc::clone(&fired);
    app.root()
        .plugin(fn_plugin("once-demo", &[], move |ctx| {
            let fired = Arc::clone(&fired2);
            async move {
                ctx.once::<u32, _, _>("ping", move |_ctx, _v: Arc<u32>| {
                    let fired = Arc::clone(&fired);
                    async move {
                        fired.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })?;
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    app.root().emit("ping", 1u32).unwrap();
    app.root().emit("ping", 2u32).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(fired.load(Ordering::SeqCst), 1);
    app.shutdown().await;
}

// —— 隔离作用域 ——

/// isolate 派生的上下文里可以提供同名服务的另一套实现，
/// 且不影响父作用域。
#[tokio::test]
async fn isolate_scopes_service_resolution() {
    let app = App::new().unwrap();

    app.root()
        .plugin(service_plugin(RootCounter))
        .unwrap()
        .ready()
        .await
        .unwrap();

    let isolated = app.root().isolate("counter");
    isolated
        .plugin(fn_plugin("iso-provider", &[], |ctx| async move {
            ctx.provide("counter", IsolatedCounter)?;
            Ok(())
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    // 父作用域看到的仍是根实现（向下转型成功即身份断言）
    let _root_value: Arc<RootCounter> = app.root().get::<RootCounter>("counter").unwrap();
    // 隔离作用域看到的是新实现
    let _iso_value: Arc<IsolatedCounter> = isolated.get::<IsolatedCounter>("counter").unwrap();
    // 反向不可见：隔离区看不到根实现
    assert!(isolated.get::<RootCounter>("counter").is_none());

    app.shutdown().await;
}

/// 根作用域标记实现。
struct RootCounter;

impl Service for RootCounter {
    fn name(&self) -> &str {
        "counter"
    }
}

/// 隔离作用域的替换实现。
struct IsolatedCounter;

impl Service for IsolatedCounter {
    fn name(&self) -> &str {
        "counter"
    }
}

// —— 副作用 ——

/// Disposer 提前撤销后，fiber 卸载时不会重复执行。
#[tokio::test]
async fn early_dispose_prevents_double_cleanup() {
    let app = App::new().unwrap();
    let runs = Arc::new(AtomicUsize::new(0));

    let runs2 = Arc::clone(&runs);
    app.root()
        .plugin(fn_plugin("guarded", &[], move |ctx| {
            let runs = Arc::clone(&runs2);
            async move {
                let disposer = ctx.effect(
                    "single-cleanup",
                    Some(Box::new(move || {
                        runs.fetch_add(1, Ordering::SeqCst);
                    })),
                )?;
                disposer.dispose(); // 提前撤销
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    app.shutdown().await;
    assert_eq!(runs.load(Ordering::SeqCst), 1, "清理只应执行一次");
}

// —— 事件作用域（internal/service 的 isolate 过滤） ——

/// 挂一个统计 `internal/service`(counter) 命中次数的监听插件。
async fn mount_service_watcher(
    mount_point: &cordis::Context,
    name: &str,
    hit: Arc<AtomicUsize>,
    global: bool,
) {
    mount_point
        .plugin(fn_plugin(name, &[], move |ctx| {
            let hit = Arc::clone(&hit);
            async move {
                ctx.on_opt::<cordis::ServiceEvent, _, _>(
                    event::SERVICE,
                    move |_ctx, service: Arc<cordis::ServiceEvent>| {
                        let hit = Arc::clone(&hit);
                        async move {
                            if service.name == "counter" {
                                hit.fetch_add(1, Ordering::SeqCst);
                            }
                            Ok(())
                        }
                    },
                    cordis::ListenOptions {
                        global,
                        ..Default::default()
                    },
                )?;
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();
}

/// 隔离作用域内的服务变更只广播给同作用域监听器与 global 监听器，
/// 根作用域的普通监听器收不到。
#[tokio::test]
async fn service_events_respect_isolate_scope() {
    let app = App::new().unwrap();
    let same_scope = Arc::new(AtomicUsize::new(0));
    let other_scope = Arc::new(AtomicUsize::new(0));
    let global_listener = Arc::new(AtomicUsize::new(0));

    // 三个监听器都观察 internal/service，注册位置不同：
    // 1) 同作用域：在隔离上下文（label "iso-A"）下注册
    let iso = app.root().isolate_as("counter", "iso-A");
    mount_service_watcher(&iso, "iso-watcher", Arc::clone(&same_scope), false).await;
    // 2) 根作用域普通监听器：服务在隔离区变更时不应收到
    mount_service_watcher(&app.root(), "root-watcher", Arc::clone(&other_scope), false).await;
    // 3) global 监听器：豁免作用域过滤，任何作用域的变更都收到
    mount_service_watcher(
        &app.root(),
        "global-watcher",
        Arc::clone(&global_listener),
        true,
    )
    .await;

    // 在隔离作用域提供 counter：变更事件按 iso-A 作用域过滤广播
    iso.plugin(fn_plugin("iso-provider", &[], |ctx| async move {
        ctx.provide("counter", Counter(AtomicUsize::new(7)))?;
        Ok(())
    }))
    .unwrap()
    .ready()
    .await
    .unwrap();

    // 进入 Active 时的广播 + fiber 结束前不撤销，各监听器恰收一次
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(same_scope.load(Ordering::SeqCst), 1, "同作用域监听器应收到");
    assert_eq!(
        other_scope.load(Ordering::SeqCst),
        0,
        "根作用域监听器应被过滤"
    );
    assert_eq!(
        global_listener.load(Ordering::SeqCst),
        1,
        "global 监听器应收到"
    );

    app.shutdown().await;
}

// —— EffectGuard（RAII 形态） ——

/// 守卫 Drop 即撤销；逆声明序执行；fiber 卸载不重复执行。
#[tokio::test]
async fn effect_guard_cleans_up_on_drop_in_reverse_order() {
    let app = App::new().unwrap();
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));

    let j1 = Arc::clone(&journal);
    let j2 = Arc::clone(&journal);
    let j3 = Arc::clone(&journal);
    app.root()
        .plugin(fn_plugin("raii", &[], move |ctx| {
            let j1 = Arc::clone(&j1);
            let j2 = Arc::clone(&j2);
            let outer_journal = Arc::clone(&j3);
            async move {
                let _a = ctx.guard(
                    "effect-a",
                    Some(Box::new(move || j1.lock().unwrap().push("undo-a"))),
                )?;
                let _b = ctx.guard(
                    "effect-b",
                    Some(Box::new(move || j2.lock().unwrap().push("undo-b"))),
                )?;
                // 守卫随 apply 的异步帧结束而 Drop：逆声明序撤销
                outer_journal.lock().unwrap().push("apply-done");
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        *journal.lock().unwrap(),
        vec!["apply-done", "undo-b", "undo-a"],
        "守卫应在 apply 帧结束时逆声明序撤销"
    );
    app.shutdown().await;
    assert_eq!(
        journal.lock().unwrap().len(),
        3,
        "fiber 卸载不应再次执行（一次性槽位）"
    );
}

/// disarm 后效果保留到 fiber 卸载（回到 Cordis 默认语义）。
#[tokio::test]
async fn guard_disarm_defers_cleanup_to_unload() {
    let app = App::new().unwrap();
    let runs = Arc::new(AtomicUsize::new(0));

    let runs2 = Arc::clone(&runs);
    app.root()
        .plugin(fn_plugin("disarmed", &[], move |ctx| {
            let runs = Arc::clone(&runs2);
            async move {
                let guard = ctx.guard(
                    "keep-until-unload",
                    Some(Box::new(move || {
                        runs.fetch_add(1, Ordering::SeqCst);
                    })),
                )?;
                guard.disarm(); // 效果留给 fiber
                Ok(())
            }
        }))
        .unwrap()
        .ready()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 0, "disarm 后不应立即执行");
    app.shutdown().await;
    assert_eq!(runs.load(Ordering::SeqCst), 1, "fiber 卸载时执行一次");
}

// —— 内置服务自举 ——

/// registry/logger 以服务形态可寻址；registry 能枚举与诊断 PENDING。
#[tokio::test]
async fn builtin_services_are_addressable_and_diagnostic() {
    let app = App::new().unwrap();

    // 内置服务可寻址（对应原版 ctx.events / ctx.registry / ctx.logger）
    let registry = app.root().registry().unwrap();
    let _logger = app
        .root()
        .expect::<cordis::Logger>("logger")
        .expect("logger 应作为服务提供");
    let _bus = app
        .root()
        .expect::<cordis::EventBus>("events")
        .expect("events 应作为服务提供");

    // 挂一个依赖无人提供服务的插件 → PENDING，诊断 API 能答"在等什么"
    let waiter = app
        .root()
        .plugin(fn_plugin("waiter", &["no-such-service"], |_ctx| async {
            Ok(())
        }))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(waiter.state(), FiberState::Pending);
    assert_eq!(waiter.missing_dependencies(), vec!["no-such-service"]);
    let pending = registry.pending();
    assert!(
        pending.iter().any(|fiber| fiber.name() == "waiter"),
        "registry.pending 应包含 waiter"
    );
    assert!(registry.names().contains(&"waiter".to_string()));
    assert!(registry.size() >= 1);

    app.shutdown().await;
}
