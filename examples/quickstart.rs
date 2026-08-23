//! Cordis 快速上手示例：服务、依赖注入、事件与生命周期的一次完整巡览。
//!
//! 运行：`cargo run --example quickstart`
//!
//! 剧情模拟一个"agent harness"的骨架：
//! - `database` 与 `tools` 是能力服务（Service 模式挂载）；
//! - `tools` 声明依赖 `database`，框架保证它只在 database 就绪后启动；
//! - 策略插件用 waterfall 事件把关 `agent/request`（改写 + 短路两种姿势）；
//! - 观察插件用 emit 事件记录 `session/event`；
//! - 最后演示"依赖撤销 → 下游自动卸载 → 依赖恢复 → 下游自动重启"。

// trait 实现里的 `fn name(&self) -> &str` 返回字面量会触发
// unnecessary_literal_bound，但实现签名必须与 trait 一致，无法采纳建议
#![allow(clippy::unnecessary_literal_bound)]

use std::sync::Arc;

use cordis::events::Next;
use cordis::{App, Context, Service, fn_plugin, service_plugin};

/// 数据库服务：能力域的入口对象。
struct Database {
    url: String,
}

impl Database {
    fn query(&self, sql: &str) -> String {
        format!("[{}] rows for {sql:?}", self.url)
    }
}

impl Service for Database {
    fn name(&self) -> &str {
        "database"
    }
}

// 示例刻意保持线性叙事（挂载 → 分发 → 拆卸一条线读到底），不做拆分
#[expect(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> cordis::Result<()> {
    let app = App::new()?;
    let root = app.root();

    // —— 1. 能力服务：database ——
    let db_fiber = root.plugin(service_plugin(Database {
        url: "sqlite:memory".into(),
    }))?;
    db_fiber.ready().await?;
    println!("✔ database 已就绪（fiber uid = {:?}）", db_fiber.uid());

    // —— 2. 依赖注入：tools 等待 database 才启动 ——
    let tools_fiber = root.plugin(fn_plugin(
        "tools-registry",
        &["database"],
        |ctx: Context| async move {
            // get::<T> / expect::<T> 始终返回 Arc<T>；
            // inject 已保证此刻 database 就绪
            let db = ctx
                .expect::<Database>("database")
                .expect("database 已由 inject 保证就绪");
            let rows = db.query("select * from tools");
            println!("✔ tools 启动：依赖读取成功 → {rows}");
            // 工具 schema 的注册是可逆副作用：卸载时自动撤销
            ctx.effect(
                "register tool: bash",
                Some(Box::new(|| println!("↺ 工具 bash 已注销"))),
            )?;
            Ok(())
        },
    ))?;
    tools_fiber.ready().await?;
    println!("✔ tools 已就绪（依赖顺序由框架编排，无需手动排序）");

    // —— 3. waterfall：请求改写（协作式，调 next 委托） ——
    root.plugin(fn_plugin(
        "request-policy",
        &[],
        |ctx: Context| async move {
            ctx.on_waterfall::<AgentRequest, _, _>(
                "agent/request",
                |_ctx, mut request: AgentRequest, next: Next<AgentRequest>| async move {
                    println!(
                        "→ [policy] 收到请求 {:?}，追加审计标注后放行",
                        request.prompt
                    );
                    request.steps.push("audited".into());
                    next.call(request).await
                },
            )?;
            println!("✔ request-policy 已挂载（waterfall 协作层）");
            Ok(())
        },
    ))?
    .ready()
    .await?;

    // —— 4. waterfall：短路决策（单决策事件，不调 next 即否决） ——
    root.plugin(fn_plugin("request-guard", &[], |ctx: Context| async move {
        ctx.on_waterfall::<AgentRequest, _, _>(
            "agent/request",
            |_ctx, request: AgentRequest, next: Next<AgentRequest>| async move {
                if request.prompt.contains("rm -rf") {
                    println!("✋ [guard] 危险请求 {:?}，短路否决", request.prompt);
                    // 不调用 next：下游与默认行为都不再执行
                    return Ok(request);
                }
                // 无决策权时必须委托
                next.call(request).await
            },
        )?;
        println!("✔ request-guard 已挂载（waterfall 决策层）");
        Ok(())
    }))?
    .ready()
    .await?;

    // —— 5. emit：会话观察者（触发即忘） ——
    root.plugin(fn_plugin("session-log", &[], |ctx: Context| async move {
        ctx.on::<SessionEvent, _, _>(
            "session/event",
            |_ctx, event: Arc<SessionEvent>| async move {
                println!("📝 [session-log] 记录事件：{}", event.kind);
                Ok(())
            },
        )?;
        Ok(())
    }))?
    .ready()
    .await?;

    // —— 6. 分发：一次正常请求 + 一次被否决的请求 ——
    let ok_request = root
        .waterfall(
            "agent/request",
            AgentRequest {
                prompt: "list files".into(),
                steps: vec![],
            },
        )
        .await?;
    println!("✔ 请求走完瀑布：{ok_request:?}\n");

    root.waterfall(
        "agent/request",
        AgentRequest {
            prompt: "rm -rf /".into(),
            steps: vec![],
        },
    )
    .await?;
    println!("✔ 危险请求被短路（下游未执行）\n");

    root.emit(
        "session/event",
        SessionEvent {
            kind: "turn/end".into(),
        },
    )?;
    // emit 触发即忘；稍候让监听器落地
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // —— 7. 依赖撤销与恢复：卸载 database，tools 应自动卸载 ——
    println!("… 撤销 database（dispose 提供方 fiber）");
    db_fiber.dispose();
    db_fiber.ready().await?;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    println!(
        "… tools 现在的状态：{:?}（依赖消失 → 自动回到等待）",
        tools_fiber.state()
    );

    // —— 8. 优雅停机：整棵树逆序卸载 ——
    println!("… shutdown：副作用按逆注册顺序撤销");
    app.shutdown().await;
    println!("✔ 已停机，fiber 状态：{:?}", app.fiber_states());

    Ok(())
}

/// `agent/request` 瀑布事件的负载：一次模型请求的决策对象。
#[derive(Debug)]
struct AgentRequest {
    prompt: String,
    steps: Vec<String>,
}

/// `session/event` 观察事件的负载。
struct SessionEvent {
    kind: String,
}
