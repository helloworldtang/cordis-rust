# cordis-rust

Cordis 是一个**插件框架**：插件向共享上下文贡献**服务**、**类型化事件**与
**可逆副作用**。本 crate 是 [deepseek-harness](https://github.com/deepseek-ai/deepseek-harness)
（dsh）底层 vendor 的 [Cordis](https://cordis.js.org/)（TypeScript）核心语义的
Rust 移植——dsh 的每一部分（模型适配器、工具注册表、会话日志、agent loop
本身）都是 Cordis 插件，因此每一部分都可以从配置替换。

> 包名是 `cordis-rust`，库名保持 `cordis`（Rust 标识符不含连字符）：
> 依赖写 `cordis-rust`，代码里 `use cordis::…`。
> crates.io 的 `cordis-rust` 名字已被另一项目占用（见"快速上手"注意），
> 本仓库以 git 依赖分发。

## 五个核心概念

| 概念 | 一句话 | 本 crate 落点 |
|---|---|---|
| 插件是实现服务的对象 | 带 `inject` 依赖声明与 `apply(ctx)` 挂载体 | [`Plugin`] / [`Service`] trait |
| 上下文是服务的容器 | 服务占据稳定的名字，按 key 查找而非导入实现 | [`Context`]（`provide` / `get`） |
| 通过 `inject` 声明依赖 | 等服务就绪才启动，加载顺序由依赖表达 | [`Plugin::inject`] |
| 类型化事件用于通信 | `emit` / `waterfall` / `parallel` / `serial` + 同步瀑布拦截 | [`EventBus`] |
| 注册是可逆的副作用 | reload 与 teardown 时按逆序撤销 | [`Context::effect`] / [`Disposer`] |

## 快速上手

> **注意**：crates.io 上的 `cordis-rust` 是另一个同名项目
> （[redoop/cordis-rust](https://github.com/redoop/cordis-rust)），
> 与本仓库无关。请勿 `cargo add cordis-rust`——请用下方锁 tag 的
> git 依赖；引用滚动分支等于隐性升级到未审阅代码。

```toml
[dependencies]
cordis-rust = { git = "https://github.com/helloworldtang/cordis-rust", tag = "v0.2.0" }
tokio = { version = "1", features = ["full"] }
```

```rust
use std::sync::Arc;
use cordis::{fn_plugin, service_plugin, App, Context, Service};

struct Database { url: String }

impl Service for Database {
    fn name(&self) -> &str { "database" }
}

#[tokio::main]
async fn main() -> cordis::Result<()> {
    let app = App::new()?;
    let root = app.root();

    // 提供者：贡献 "database" 服务
    root.plugin(service_plugin(Database { url: "sqlite:memory".into() }))?;

    // 消费者：声明依赖，框架保证它只在 database 就绪后启动
    root.plugin(fn_plugin("migrator", &["database"], |ctx: Context| async move {
        // get::<T> / expect::<T> 始终返回 Arc<T>
        let db = ctx.expect::<Database>("database")?;
        println!("migrate on {}", db.url);
        Ok(())
    }))?;

    app.shutdown().await;   // 整棵插件树按逆序卸载
    Ok(())
}
```

完整巡览（服务、依赖、瀑布拦截、短路否决、动态卸载/重启）见
`examples/quickstart.rs`；对 Cordis 架构本质与移植取舍的完整解读见
[`docs/cordis-essence.md`](docs/cordis-essence.md)；遗留问题与路线图见
[`TODO.md`](TODO.md)：

```sh
cargo run --example quickstart
cargo test                # 27 项测试覆盖全部交互契约
```

> **运行时要求**：插件加载与事件分发依赖 tokio 运行时，所有入口须在
> runtime 上下文中调用。

---

# 架构解读

下面按**结构 → 角色 → 角色之间的关系 → 角色之间的交互规范**四个维度
解读这个框架。

## 一、结构

### 静态结构（模块划分）

```text
src/
├── lib.rs        crate 出入口：prelude、ListenOptions、lock! 宏
├── context.rs    Context（服务容器）+ App（共享运行时）：服务存储、
│                 作用域解析、插件挂载、typed 事件入口
├── fiber.rs      Fiber：插件运行实例，依赖驱动的生命周期状态机
├── registry.rs   Plugin trait + FnPlugin（闭包插件）+ Registry（运行时表）
├── events.rs     EventBus：四种分发模式 + 契约校验 + Next 瀑布续体
├── effect.rs     Disposer：一次性清理槽（副作用可逆性的最小单元）
├── service.rs    Service trait + service_plugin 适配器（"插件即服务"模式）
├── error.rs      CordisError：稳定错误码的错误层级
└── logger.rs     内置日志器（可被同名服务替换）
```

每个模块对应原版 Cordis 的同名文件（`context.ts` / `fiber.ts` /
`registry.ts` / `events.ts` / `service.ts`…），语义一一映射。

### 运行时结构

```text
App ──► AppInner（共享运行时，全树唯一）
        ├── services: {(作用域键, 服务名) → ServiceImpl{值, 提供方 fiber}}
        ├── registry: {插件实例身份 → RuntimeRecord{展示名, fibers[]}}
        ├── bus:      EventBus（事件名 → 契约 + 监听器列表）
        └── logger / pending_disposals / scope_labels …

App ──► root fiber (uid 0, Active)
          │
          └── ctx = 根上下文 ─────────────────────────────┐
              │ ctx.plugin(A) → fiber A ── A 的插件 ctx    │ 所有 ctx
              │     │ ctx.plugin(B) → fiber B ── B 的 ctx  │ 共享同一个
              │     │                                      │ AppInner
              └──── …（子 fiber 的 ctx 沿 parent 链向上）◄┘
```

要点：

- **一棵 fiber 树**：每次 `ctx.plugin()` 产生一个 fiber，其存活登记为
  父 fiber 的副作用——父卸载时子先走，整棵树的卸载顺序恰与挂载顺序相反；
- **一条上下文链**：每个 fiber 拥有自己的 `Context`（`Arc` 句柄，克隆
  廉价），沿 `parent` 链向上解析服务与作用域；
- **一份共享状态**：服务存储、注册表、事件总线都住在 `AppInner`，
  隔离作用域只是给"服务名 → 作用域键"的解析多了一层覆盖。

## 二、角色

### App —— 应用入口

持有共享运行时（`AppInner`）与根 fiber，是整棵插件树的生命锚点。
`App::new()` 构造根上下文并安装内置服务（`events`）；`shutdown()` 逆序
销毁全树并等待清理收尾。诊断接口：`plugin_count()`、`fiber_states()`。

### Context —— 服务容器（对应原版 context proxy）

代表"某个 fiber 视角的容器切片"，是用户代码的主要抓手：

- **服务**：`provide`（写入，返回 [`Disposer`]）/ `get::<T>`（返回
  `Option<Arc<T>>`）/ `expect::<T>`（返回精确错误）；
- **插件**：`plugin`（挂载）/ `inject`（挂载等待依赖的闭包插件）；
- **服务**：`provide` / `provide_with_check`（带可用性谓词，只影响依赖
  就绪判定）/ `set`（提供方原位覆盖自己的服务值）注册与改写；
- **副作用**：`effect`（登记清理动作）；
- **事件**：`on` / `once` / `on_waterfall` / `on_serial` 注册，
  `emit` / `parallel` / `serial` / `waterfall` 分发；
- **拦截**：`on_waterfall_sync` 注册同步瀑布监听器，挂到
  `internal/get` / `internal/set` 即可 shadow / fallback / 否决服务读写；
- **作用域**：`isolate` / `isolate_as` 派生隔离子上下文。

原版用 Proxy 让 `ctx.foo` 即服务读取；Rust 没有动态属性，服务读取统一
走 `ctx.get::<T>("foo")`——"读取"与"依赖等待"由此解耦（后者由 `inject`
全权承担）。

### Plugin —— 插件

```rust
pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &str { "anonymous" }
    fn inject(&self) -> Vec<String> { Vec::new() }
    fn apply(&self, ctx: Context) -> BoxFuture<'static, Result<()>>;
}
```

原版支持函数/类/`{apply}` 对象三种形态，Rust 收敛为一个 trait。闭包场景
用 [`fn_plugin`]（对应原版函数插件与 `ctx.inject(deps, callback)` 速记）。

### Service —— 服务本体

"插件是实现 Service 的对象"在 Rust 落成两半：**服务本体**实现
`Service` trait（名字 + `start`/`stop` 钩子），**挂载**交给
[`service_plugin`] 包装的适配插件——`apply` 先把 `Arc<S>` 提供到 ctx，
再跑 `start`；卸载时先 `stop` 再撤销注册。消费方 `ctx.get::<Self>(name)`
得到 `Arc<Self>`。

### Fiber —— 插件运行实例（生命周期状态机）

```text
           依赖就绪                     apply 成功
 Pending ──────────► Loading ──────────────► Active
    ▲                   │ apply 失败            │ 依赖被撤销
    │ 依赖恢复           ▼                      ▼
    └────────────── Failed ◄──── (撤销已注册副作用)
    │                                          │
    └──────────── Unloading ◄──────────────────┘
                      │ 清理完成
                      ▼
                   Disposed（终态，来自 dispose()）
```

`ctx.plugin()` 同步返回 [`Fiber`] 句柄，`apply` 在后台异步运行；
`fiber.ready().await` 等待收尾并重抛启动错误；`fiber.dispose()` 卸载。

### Registry / RegistryService —— 插件注册表

以**插件实例身份**（`Arc` 指针）为键跟踪运行时记录：同一实例多次挂载
共享一条记录、各得一个 fiber；最后一个 fiber 移除后记录一并删除。
注册表以**内置服务 `registry`** 的形态挂载在根上下文
（[`RegistryService`]）：任何插件可枚举运行时记录、fiber 状态与
Pending 队列，配合 [`Fiber::missing_dependencies`] 回答
"谁在等什么"——依赖驱动加载的另一面是静默等待，诊断靠这套 API。

### EventBus 与监听器 —— 事件总线

事件的**分发模式是公开契约**：每个事件名自首次注册起绑定一种监听形态
（观察 / 瀑布 / 串行）与一种负载类型，冲突即错
（[`CordisError::EventModeMismatch`] / [`EventPayloadMismatch`]）。
监听器收到 `(派发方上下文, 负载)`；[`Next`] 是瀑布事件的续体。
监听器注册时捕获其上下文的**隔离快照**：`internal/service` 这类作用域
敏感的派发只送达同作用域（或声明 `global`）的监听器。

### Effect / Disposer / EffectGuard —— 可逆副作用

副作用的最小单元 = 一次性清理槽（`Arc<Mutex<Option<Cleanup>>>`）。
注册产生两条消费路径——fiber 卸载与手动 `dispose`——先到先得，
只执行一次。卸载时按**逆注册顺序**执行。

两种句柄形态表达同一套槽位：

- [`Disposer`]（`ctx.effect`，JS 语义）：句柄被丢弃 ≠ 效果撤销，
  效果存活到 fiber 卸载；
- [`EffectGuard`]（`ctx.guard`，Rust 母语）：**Drop 即撤销**，与局部
  变量的逆声明序天然一致；需要回到 Cordis 语义时 `disarm()`
  （留给 fiber）或 `detach()`（拿回 Disposer 显式管理）。

### Scope —— 隔离作用域

`ctx.isolate("tools")` 派生的子上下文里，`tools` 这个服务名解析到新
作用域：可以提供另一套实现而不影响父作用域；`isolate_as(name, label)`
让同名 label 的派生共享作用域（scope join）。作用域同时约束
`internal/service` 事件的可见范围（见"交互规范"）。

## 三、角色之间的关系

```text
            创建/持有（实线 = 强引用保活）
 App ─────────────► AppInner ◄──────────────┐（全部 ctx 共享）
  │                      ▲                   │
  │ 持有 root            │ 访问              │
  ▼                      │                   │
 root fiber ──────► root ctx ────────────────┤
  │ effects[] 保活          │ parent 链       │
  ▼                         ▼                │
 child fiber ──────► child ctx ──────────────┘
  │        ▲ 弱引用（ctx.fiber）
  │ provide
  ▼
 ServiceImpl{ value, fiber(弱) } ──► 消费方 ctx.get 沿作用域解析

 Registry ──► fibers[]（strong）──► 插件本体（fiber.plugin 保活）
 EventBus ◄─ on() 注册 ── Disposer 挂到 fiber.effects[]
```

- **App ↔ 根 fiber ↔ 根 ctx** 互为锚点：ctx 对 fiber 只持弱引用，
  App 持强引用——用户放掉 App 与全部 ctx 后整棵树可被释放，无循环泄漏；
- **父 fiber → 子 fiber**：`ctx.plugin()` 把"卸载子 fiber"登记为父的
  副作用槽，因此子 fiber 的生命周期完全由父托管；
- **服务 → 提供方 fiber**：`ServiceImpl` 持提供方弱引用。解析时要求
  提供方处于 `Active`——这是"服务可见 = 提供方已就绪"的保证；
- **监听器 → fiber**：`ctx.on()` 在注册的同时把"移除监听器"登记为
  当前 fiber 的副作用，fiber 卸载时监听器随之一并撤销；
- **Registry ↔ 插件**：记录存在 ⇒ fiber 存在 ⇒ 插件实例存活，
  因此注册表只需持 fiber，不必持插件。

一句话：**除了 App 与用户代码，一切都由 fiber 的副作用表保活**；
这也解释了为什么卸载是"逆注册顺序"——它就是父 fiber 副作用表的倒序遍历。

## 四、角色之间的交互规范

### 1. 服务提供与消费

- 服务以**名字**为键、以 `Arc<dyn Any + Send + Sync>` 为值存储；
  `get::<T>` / `expect::<T>` **始终返回 `Arc<T>`**，无论提供方用的是
  `provide`（按值）还是 `provide_arc`（共享句柄）；
- 同一作用域内服务名不可重复占用，否则
  [`CordisError::AlreadyProvided`]（带先占方名字）；
- **提供方 Active 才可见**：Loading 期间注册的服务，要等 fiber 进入
  Active 才对消费方可见——消费方永远读不到半初始化的服务；
- 类型不符时 [`CordisError::ServiceTypeMismatch`] 携带注册类型名与
  请求类型名，注册与消费两端的类型漂移一眼可见。

### 2. 依赖等待（inject）

- 插件声明 `inject(["database"])` 后，**只有这些服务全部就绪（且提供方
  Active）才加载**；任一依赖被撤销则自动卸载、回到 Pending，依赖恢复后
  自动重新加载；
- 加载顺序通过依赖表达，**而非手动编排**——先挂载消费者、后挂载提供者
  也一样正确（消费者只是先在 Pending 等待）；
- `apply` 失败的 fiber 进入 `Failed`（副作用已回滚），依赖变化仍可再次
  拉起它。

### 3. 事件分发（模式即契约）

| 模式 | 是否等待 | 顺序 | 返回值 | 注册方法 |
|---|---|---|---|---|
| `emit` | 否（触发即忘） | 注册顺序 | 无（错误记日志） | `on` / `once` |
| `waterfall` | 是（async） | 注册顺序，先注册者在外层 | 链条最终值 | `on_waterfall` |
| `parallel` | 是 | 并发扇出 | 无（聚合错误） | `on` / `once` |
| `serial` | 是 | 注册顺序 | 首个 `Some`（bail） | `on_serial` |
| `waterfall(sync)` | 是（同步） | 注册顺序，先注册者在外层 | 链条最终值 | `on_waterfall_sync` |

- **一个事件一种形态**：`emit`/`parallel` 共享观察形态；`waterfall`、
  `serial`、`waterfall(sync)` 各有专属形态。混用即 `EventModeMismatch`；
  负载类型漂移在注册时即被拒绝；
- **waterfall 规则**：监听器收到 `(派发 ctx, 值, next)`。改写值后必须
  `next.call(value)` 委托下游；不调用而直接返回即**短路**——下游与
  链尾默认行为都不执行。单决策事件里短路是设计意图（策略监听器拥有
  决策权时不委托）；仅观察/标注的监听器必须委托；
- **同步瀑布（拦截层）**：`on_waterfall_sync` 注册的监听器整条链同步
  执行（对应原版 Proxy trap 内不能异步的 waterfall）。框架用它实现
  服务读写拦截（见下表 `internal/get` / `internal/set`），shadow /
  fallback / 否决都在这一层；
- **prepend 慎用**：仅当监听器必须先于普通注册运行时设置
  （`ListenOptions { prepend: true }`，waterfall 中即更外层）；
- **once**：监听器在派发快照阶段同步摘除，一次派发恰好消费一次；
- 监听器收到的 `ctx` 是**派发方**（事件来源），不是注册方；需要自己的
  上下文应在注册时闭包捕获；
- **作用域可见性**：监听器注册时捕获其上下文的隔离快照。普通业务事件
  的分发不过滤；`internal/service` 按服务作用域过滤——隔离区内服务的
  变更事件只送达同作用域监听器，`ListenOptions { global: true }` 豁免。

### 4. 副作用与撤销

- 每个注册都有对应的 disposer：`ctx.effect` / `ctx.guard` 登记清理、
  `ctx.on` 自动登记、`ctx.provide` 自动登记、`ctx.plugin` 登记子 fiber
  卸载；
- **逆注册顺序撤销**；需要保证"先后手"的工作放在同一个 effect 里；
- disposer 与 fiber 卸载**竞争时只执行一次**；
- **失败即清理**：`apply` 失败的 fiber 不残留半注册的副作用；
- 两种句柄按需选用：`ctx.effect` 返回 [`Disposer`]（丢弃≠撤销，
  Cordis 语义），`ctx.guard` 返回 [`EffectGuard`]（Drop 即撤销，
  Rust 母语；`disarm` 回到前者语义）；
- 清理是同步动作；需要异步收尾的工作放在服务的 `stop` 钩子。

### 5. 隔离作用域

`isolate(name)` / `isolate_as(name, label)` 之下，`name` 的提供与解析
都落在新作用域：子树可以整体替换一个实现而父作用域无感；具名作用域
可被多处派生共享（同一 label = 同一作用域）。作用域同时约束
`internal/service` 事件的可见范围。

### 6. 框架内置事件与服务

| 事件名 | 负载 | 时机 |
|---|---|---|
| `internal/plugin` | `Fiber` | 有插件 fiber 被创建 |
| `internal/status` | `StatusEvent{fiber, prev, now}` | fiber 状态变化 |
| `internal/service` | `ServiceEvent{name, value}` | 服务提供/撤销（**按作用域过滤**） |
| `internal/get` | `ServiceGet{name, value}`（同步瀑布） | `get` / `expect` 读取服务：可改写（shadow）、补默认（fallback）、清空（否决）；框架内部依赖解析不经此层 |
| `internal/set` | `ServiceSet{name, value}`（同步瀑布） | `provide` 写入前：可改写名/值（终态才是实际写入与撤销的键值），或 `Err` 否决 |
| `internal/dispatch` | `DispatchEvent{mode, name}` | 公开事件分发前的统一观测点（`internal/*` 自身豁免防递归） |

框架自举为三个内置服务（名字被占用，对应原版构造函数里安装
events/logger/registry 的顺序）：`events`（事件总线本体）、
`registry`（[`RegistryService`]，枚举与 Pending 诊断）、`logger`。

### 7. 错误处理

- 框架自身**没有 panic 路径**：锁中毒走恢复（清理动作全部
  `catch_unwind` 保护），错误一律 `Result<CordisError>`；
- 每个错误变体对应一个稳定错误码（`CordisError::code()`），对应原版
  `CordisError.Code`；
- 插件启动失败不炸掉宿主：错误被记录、fiber 转入 `Failed`，
  `fiber.ready()` 向调用方如实重抛；
- `parallel` 分发的监听器失败被聚合为 `ParallelDispatchFailed`；
  `emit` 模式的监听器失败只记日志（触发即忘的代价）。

## 与原版 Cordis 的差异

| 原版 | Rust 版 | 原因 |
|---|---|---|
| 函数/类/`{apply}` 三种插件形态 | 单一 `Plugin` trait + `fn_plugin` 闭包适配 | 类型系统已排除非法形态 |
| `ctx.foo` proxy 属性读取 | `ctx.get::<T>("foo")` | Rust 无动态属性 |
| 配置经 schema 校验、`fiber.update()` 热更新 | 配置在插件构造期捕获 | v1 简化；热更新列为后续工作 |
| `inject` 可携带 intercept 配置 | `inject` 只有服务名 | 配置注入未移植（属配置接缝）；服务读写拦截已以 `internal/get`/`internal/set` 同步瀑布补齐 |
| disposer 可为异步/Promise/生成器 | 清理为同步闭包 | 大多数清理是注销动作；异步收尾放 `Service::stop` |
| fiber 名继承最近具名祖先 | 直接用插件名 | 简化诊断 |
| `bail` 分发（同步版 serial） | 未实现 | 与 serial 语义重合 |
| HMR / loader / 配置层叠 | 未实现 | 属于 dsh 组合层，不在框架核心 |

### 差异的层级

上表并非同质差异，按影响深度分三层：

**语言机制层（架构语义不变）**——插件形态收敛与 proxy→`get` 只换了
表达方式：角色、契约、解析算法（作用域链 + 提供方 Active 可见）与原版
一致。原版三种插件形态本就是同一契约的三种 TS 入口。

**架构语义层（交互契约实质收紧）**——三条真正值得注意的：

1. **配置模型**：原版配置是运行时数据（schema 校验 + 热更新 + intercept
   注入），是 dsh"组合包可 patch"的支柱；Rust 版配置在构造期捕获，
   "配置即扩展点"的性质随之消失；
2. **清理同步化**：原版 `provide` 的清理会 await 下游依赖方 settle 后
   再清自身存储；同步闭包做不到——服务撤销瞬间依赖方可能仍有一小段
   在跑（随后被 refresh 拉去卸载）。实现约束传导为卸载顺序保证的弱化；
3. **inject 强制性**：原版未声明 inject 就读服务在 proxy 层报错（运行
   时强制依赖声明）；Rust `get` 自由读取，护栏从机制强制降为约定。

**范围层（未实现 ≠ 架构分歧）**——bail 语义被 serial 完全覆盖；
HMR/loader 本就不在 cordis 核心包（属生态上层）；fiber 名继承纯属诊断
展示。

移植的总原则：**角色、关系、核心交互契约（四模式表、waterfall next
规则、逆序撤销、依赖驱动生命周期、isolate）逐条保真；凡语义依赖 JS
动态机制（Proxy、prototype 链、运行时 schema、异步 effect）且保真成本
高于 v1 收益的，收紧语义并在此显式记录**。

### 审计后已修复的偏差

v1 完成后做过一轮源码级审计（详见 [`docs/cordis-essence.md`](docs/cordis-essence.md)），
修正了两处"照猫画虎"并补上 Rust 母语惯用法：

- **事件作用域**：`internal/service` 现按服务作用域过滤派发
  （隔离区的变更不再广播全世界），`ListenOptions.global` 豁免；
- **内置能力自举**：`registry` / `logger` 与 `events` 一样以服务形态
  挂载到根上下文，可寻址、可注入；新增 `Fiber::missing_dependencies`
  与 `RegistryService::pending` 诊断静默 PENDING；
- **RAII 守卫**：`ctx.guard` 返回 Drop 即撤销的 [`EffectGuard`]，
  与 Cordis 语义的 [`Disposer`] 并存；
- **读写拦截瀑布**：原版 Proxy trap 内的 `internal/get` / `internal/set`
  同步 waterfall 已移植为 `on_waterfall_sync`（读原版源码确认其瀑布
  本就是同步形态——Proxy trap 物理上不能异步），shadow / fallback /
  否决语义与原版一致，框架依赖解析不受读拦截影响；
- **provide 直供唤醒**：根上下文直接 `provide` 此前只广播事件、不唤醒
  下游（唤醒只发生在插件 fiber 转 Active 的路径上）——与原版 provide
  在 ACTIVE 态 `notify([name])` 不符，已补 `notify_service_change`；
- **可用性谓词**：`provide_with_check` 补齐原版 `Impl.check` /
  `_checkImpl`——谓词只参与依赖就绪判定，不影响读取；谓词 panic 按
  未就绪处理并记日志；
- **原位覆盖**：`set` 补齐原版 `reflect.set`——仅提供方可改写自己的
  服务值，不重新广播；
- **分发观测**：`internal/dispatch` 补齐原版 dispatch 内的统一观测点
  （公开事件分发前携带模式与事件名触发）。

仍未补的（记录在理解文档的路线图）：typed 服务键、配置接缝。

## 未实现清单（寻求方案）

v1 相对原版 Cordis 的核心语义已全部落地并有测试锚定；下面是**尚未实现
或有语义弱化**的部分，按"离 Cordis 本质的距离"排序，每条注明当前阻碍。
如果你对其中某条有成熟方案（或想直接认领），欢迎开 issue / PR 讨论。

### 架构债（"无特权内核"的最后拼图）

- [ ] **typed 服务键（双轨）**
  `TypeId` 主键消 typo + `trait ServiceKey { const NAME }` 保诊断与
  未来配置层寻址。当前纯字符串键的收益（配置文件可寻址）因配置层缺失
  而归零，typo 成本全在。参考 Bevy Resources 的路线。

### 语义弱化（已记录、待条件成熟再补）

- [ ] **provide 撤销不等待下游 settle**：原版 provide 的 disposer 会
  await 依赖方 fiber 收尾后再清自身存储；同步 Cleanup 做不到。补法：
  等 Rust async Drop 落地，或 provide 的清理 spawn 一个等待任务后再
  删存储（需处理与二次 provide 的竞态）。
- [ ] **fiber 依赖唤醒过宽**：`notify_service_change` 唤醒所有注入该
  服务名的 fiber 各自重估，原版按作用域过滤。正确性等价，多算几次
  依赖解析；服务数大后才值得收窄。
- [ ] **emit 时序弱于原版**：原版 emit 同步执行监听器的同步部分；
  Rust 版整体 spawn。对依赖 emit 顺序的观察者有影响。备选：提供
  `emit_ordered`（无错误的 parallel 语义，按序 await）。
- [ ] **inject 强制性降级为约定**：原版未声明 inject 读服务在 proxy
  层报错；Rust `get` 自由读取。候选护栏：debug 构建下 get 未命中时
  打 warn（携带读取方 fiber 名与服务名）。

### 功能缺口（依赖前两项架构债）

- [ ] **配置接缝**（前置：typed 键）：描述符/工厂形态插件 +
  `fiber.update(Value)` + `internal/update` 瀑布 + `InjectDecl` 带
  intercept 配置。补齐后 loader/patch/HMR 可在不碰核心的前提下长
  出来。代价：`Plugin` trait 一次 breaking change（身份/状态拆分）。
- [ ] **proc-macro 装饰器**：`#[inject("database")]` /
  `derive(Plugin)`，对应原版 `@Inject()`。需新开 `cordis-macros` crate。

另有若干"有意不移植"项（bail、HMR/loader 等范围层差异）见上文
["与原版 Cordis 的差异"](#与原版-cordis-的差异)；完整带位置的清单
（含防回归备忘）见 [`TODO.md`](TODO.md)。

## License

MIT（见 [`LICENSE`](LICENSE)）。
