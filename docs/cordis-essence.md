# 理解 Cordis：本质、移植与取舍

> 本文是对 Cordis（deepseek-harness 底层 vendor 的 TypeScript 插件框架）
> 的架构理解沉淀，以及 Rust 移植版的取舍记录。README 回答"这个 crate
> 怎么用、由哪些角色组成"；本文回答"**Cordis 凭什么是这个形状**、
> Rust 版保住了什么、丢了什么、为什么"。
>
> 原版源码参照 `vendor/cordis/src/`（context.ts / fiber.ts / registry.ts /
> events.ts / reflect.ts / service.ts / utils.ts），语义解读参照 dsh 的
> `docs/cordis-primer.zh.md` 与 `docs/cordis-tutorial/`。

## 一、本质：一句话与三个支柱

**Cordis 是一棵可撤销的插件树：ctx 既是能力通道又是所有权作用域，
依赖即加载顺序，连框架自身都是插件。**

拆成三个支柱：

1. **树即事务边界（可逆性）**。每个 fiber 是一个作用域，其上的一切
   注册——服务、监听器、子 fiber——都是副作用，随 fiber 卸载按逆注册
   顺序撤销。卸载一棵子树 = 回滚一段历史。"把行为装进插件"的回报
   不是复用，而是**可撤销**：HMR 靠它换代码，dsh 靠它换配置组合。
2. **图由声明涌现（延迟绑定）**。插件声明 `inject`，框架等依赖就绪才
   加载、依赖消失就卸载。没有人手写启动顺序；先挂消费者后挂提供者
   也一样正确——消费者只是在 Pending 里等一会儿。**副作用是静默的**：
   依赖无人提供时 fiber 永远等待且不报错（教程专设一节讲这个诊断
   场景），所以 registry 枚举是必需的公开能力，不是锦上添花。
3. **无特权内核（自举）**。`ctx.on`/`ctx.emit` 这些"框架方法"本身是
   reflect 服务用 mixin 装上去的；`events`/`logger`/`registry` 是构造
   时挂载的服务；连 loader 本身也是插件（教程实证：诊断时能看到
   Loader、Include 处于 ACTIVE）。框架自身操作也有扩展点
   （`internal/get`、`internal/set`、`internal/listener`、`internal/config`、
   `internal/update` 全是瀑布事件）。**"不存在需要打补丁的特权内核"
   不是修辞，是物理事实**：换掉一个提供方就能改变整个产品，
   这正是 dsh "每一部分都可从配置替换"的地基。

## 二、五个核心概念的机制细节

### 插件（Plugin）

- 原版三种形态（函数 / 类 / `{apply}` 对象）是**同一契约的三个 TS 入口**；
  registry 用 `resolve()` 把它们都归一到 callback（身份键）。
- `inject` 是插件与框架的全部约定：数组或"服务名 → intercept 配置"映射。
  intercept 让消费方向自己依赖的服务注入配置——DI 不止是"拿到实现"，
  还包括"配置它"。
- `apply` 可以返回 disposer / Promise / 生成器——生成器形态（挂起持有
  多个 disposer）是"effect 的生命周期 = 协程的生命周期"的表达。

### 上下文（Context）

- 原版是 prototype 链 + Proxy：`ctx.foo` 属性读取触发服务解析，沿
  fiber 链上溯（`fiber.store`），受 isolate 作用域约束，未 inject 读取
  会抛错——**proxy 是强制依赖声明的执行点**。
- `extend()` / `isolate()` / `intercept()` 派生子上下文而不改父：
  子上下文 = 父 + 一层覆盖。isolate 的实现极其轻：只是给某个服务名
  换一个 prototype 层的符号键。
- 上下文同时是**事件的过滤器载体**：dispatch 时 thisArg 携带的
  `Context.filter` 决定哪些 hook 可见（`internal/service` 就靠它做
  作用域广播），`global: true` 豁免。

### Fiber（生命周期）

- 状态机：`Pending → Loading → Active →（失败）Failed /（依赖消失）
  Unloading → Pending / Disposed`。所有转换由**依赖满足情况**驱动
  （`_checkImpl` + `_refresh` + epoch），加载/卸载转换互斥（`inertia`
  槽位串行化）。
- 子 fiber 的 `dispose` 被登记为父 fiber 的 effect——**树形 teardown
  就是父副作用表的倒序遍历**，没有独立的编排器。
- 失败即清理：`apply` 抛错 → epoch 置 INACTIVE → `_unload` 回卷已注册
  的副作用 → Failed。半初始化状态不会外泄。

### 服务（Service）

- 服务 = 名字 + 值 + 提供方 fiber + 可选 `check` 谓词。存储在
  `(isolate 作用域键, 名字)` 的二维空间里；解析要求提供方 Active
  （严格模式）——**消费方永远读不到半初始化的服务**。
- `Service` 基类模式：构造即注册（`ctx.provide(name, this)`），
  `[Service.invoke]` 让服务可调用（`ctx.logger(name)`），
  `[Service.resolveConfig]` 沿 ctx 链合并 intercept 配置。
- provide 的 disposer 会**等依赖它的下游 fiber settle 之后**再清自身
  存储——"提供方先走"不会留下读悬空服务的窗口。

### 事件（Events）

- 分发模式是事件契约的一部分：`emit`（不等待）/ `waterfall`（洋葱
  包装，`next()` 委托、不调即否决）/ `parallel`（并发扇出，聚合错误）/
  `serial`（按序，首个 bail 值短路），另有同步版 `bail`。
- waterfall 的定位是**环绕中间件**：协作式监听器改写共享决策对象后
  委托；拥有决策权的监听器不调 `next()` 直接返回即短路。dsh 把
  `agent/pre-step`、`agent/request`、`tools/*` 都做成 waterfall，
  "拦截和策略优先使用事件"说的就是它。
- 监听器归 fiber 所有（`ctx.fiber.effect` 包住注册），卸载即撤销；
  监听器收到的是**派发方上下文**（thisArg），不是注册方。

## 三、作用域：一个概念，三处生效

isolate 不只是"服务换实现"。同一个作用域概念在三处生效，这是审计中
发现的 v1 最大遗漏：

| 生效点 | 机制 | 语义 |
|---|---|---|
| 服务解析 | `(作用域键, 名字)` 存储 + fiber 链上溯 | 隔离区提供/读取另一套实现 |
| 依赖唤醒 | `notify(names, filter)` 按作用域过滤 fiber | 只有同作用域的等待者被唤醒 |
| 事件可见 | dispatch 携带 `Context.filter` 过滤 hook | `internal/service` 只广播给同作用域监听器 |

只移植第一处，"子树即世界"只对服务成立、对事件不成立——隔离区里发生
的服务变更会漏到别的子树。Rust 版已补齐第三处（Hook 携带注册方隔离
快照，`emit_scoped` 过滤派发，`ListenOptions.global` 豁免）；第二处
用"唤醒全部注入者、各自重估"的过宽实现替代（正确性等价，多算几次
依赖解析）。

## 四、Rust 移植的取舍地图

差异不是同质的，分三层判断（详见 README"差异的层级"）：

**语言机制层（语义不变）**——插件三形态收敛为单 trait（原版本就是
同一契约的三个入口）；Proxy 属性读取改为 `ctx.get::<T>("foo")`
（解析算法一致：作用域链 + 提供方 Active 可见）。

**架构语义层（契约实质收紧）**——三条最大的：

1. **配置模型**。原版配置是运行时数据：schema 校验、`fiber.update()`
   热重载、loader 按 `id` 插值、`disabled` 保留条目只卸载、intercept
   合并注入——这是 dsh "组合包可 patch、一切从配置替换"的支柱。
   Rust 版配置在插件构造期捕获为所有权数据，"配置即扩展点"的性质
   随之消失。复盘结论：砍 loader 时连机制一起砍了，更忠实的切法是
   **砍驱动、留机制**；保真的硬阻碍是"插件=实例"把身份与状态耦死
   （同一 `Arc<dyn Plugin>` 挂两次无法各持一份配置），需要描述符/
   工厂形态的 trait 重造。
2. **清理同步化**。原版 disposer 可异步，且 provide 撤销会 await 下游
   settle；Rust 无 async Drop（语言缺口），同步闭包做不到等待——
   服务撤销瞬间依赖方可能还有一小段在跑。实现约束传导为卸载顺序
   保证的弱化，是"实现原理不同改变交互契约"的最清晰案例。
3. **inject 强制性**。原版未 inject 读服务在 proxy 层报错（运行时
   强制依赖声明）；Rust `get` 自由读取，护栏从机制强制降为约定。

**范围层（未实现 ≠ 架构分歧）**——bail 被 serial 覆盖；HMR/loader
本在生态上层；fiber 名继承纯属诊断。

取舍总原则：**角色、关系、核心交互契约（四模式表、waterfall next
规则、逆序撤销、依赖驱动生命周期、isolate 三处生效）逐条保真；凡
语义依赖 JS 动态机制且保真成本高于当前收益的，收紧并显式记录。**

## 五、审计与修复记录

v1 完成后做过一轮源码级审计（对照 `vendor/cordis/src` 逐文件复查 +
Rust 生态检索），发现两处"照猫画虎"并已修复：

| 偏差 | 根因 | 修复 |
|---|---|---|
| 事件游离在作用域体系外 | 只移植了 isolate 的服务解析一处 | Hook 携带注册方隔离快照；`internal/service` 经 `emit_scoped` 按服务作用域过滤；`ListenOptions.global` 豁免 |
| `AppInner` 特权内核 | README 宣称"无特权内核"但 registry/日志/服务解析焊死在内部 | `registry`（RegistryService：size/names/fibers/pending）与 `logger` 以服务形态挂载到根，`events` 原本已是；`Fiber::missing_dependencies` 补 PENDING 静默诊断 |
| Disposer 走 JS GC 语义 | 照原版形，未用宿主语言母语 | `ctx.guard` 返回 `EffectGuard`：Drop 即撤销（与局部变量逆声明序一致），`disarm()` 回到 Cordis 语义，`detach()` 拿回显式管理；两形态共享同一一次性槽位 |

审计同时确认了两个"看似偷懒实为正确"的选择：`async fn` in trait 至今
不可 dyn（手动 `Pin<Box<dyn Future>>` 是社区标准姿势）；Rust 没有
async Drop（同步 Cleanup + `Service::stop` 钩子是语言约束下的正解）。

## 六、路线图（未竟事项）

> 完整的遗留问题清单（含阻碍说明与防回归备忘）维护在
> [`TODO.md`](../TODO.md)，本节只列主线。

按"离本质的距离"排序：

1. **typed 服务键**——`TypeId` 主键消 typo，`&'static str` 保配置
   寻址（`trait ServiceKey { const NAME }` 双轨）。Bevy Resources 证明
   了 Rust 生态的主流路线；字符串键的价值在配置层，而配置层暂缺。
2. **配置接缝**——描述符/工厂形态插件 + `fiber.update(Value)` +
   `internal/update` 瀑布 + `InjectDecl` 带 intercept 配置。补齐后
   loader/patch/HMR 可以在不碰核心的前提下长出来；代价是 Plugin trait
   一次 breaking change。
3. **proc-macro 装饰器**——`#[inject("database")]` / `derive(Plugin)`，
   对应原版 `@Inject()`。

已完成的路线图项（2026-10）：

- **`internal/get` / `internal/set` 拦截瀑布**——以**同步瀑布监听形态**
  落地（`on_waterfall_sync` / `SyncNext`）。方案考古结论：原版
  waterfall 本就是同步的（`events.ts` 无 await，Proxy trap 物理上不能
  异步），"Rust get 同步承载不了瀑布"的早期认知是反的；且整个 dsh
  生态 200+ 包无插件监听这对事件（扩展点存在但未被使用）。同一轮
  源码审计还补齐了 `provide_with_check`（`Impl.check`）、`set`
  （`reflect.set`）、`internal/dispatch` 观测事件，并修复了根上下文
  直供不唤醒下游的偏差（原版 provide 在 ACTIVE 态会 `notify`）。

## 七、原版 ↔ Rust 文件对照

| 原版（vendor/cordis/src） | Rust（src/） | 保真度 |
|---|---|---|
| `context.ts`（Context/proxy/isolate/intercept） | `context.rs`（Context/App/AppInner/isolate） | 语义保真；proxy→显式 get；intercept 未移植 |
| `fiber.ts`（Fiber/状态机/effect/inertia） | `fiber.rs`（Fiber/FiberInner/refresh/run） | 保真；update/HMR 除外 |
| `registry.ts`（Plugin 形态/Registry） | `registry.rs`（Plugin trait/FnPlugin/Registry） | 保真；键 = 实例指针 vs callback |
| `events.ts`（五模式/filter） | `events.rs`（四模式+同步瀑布/Contract/作用域过滤） | emit/waterfall/parallel/serial + 同步瀑布（internal/get、set、dispatch）+ 过滤保真；bail/internal-listener 未移植 |
| `reflect.ts`（服务存储/notify/mixin/accessor） | `context.rs` 服务部分 + `RegistryService` | 存储/通知/check 谓词/set/internal-get-set 保真；mixin/accessor 属语言机制未移植 |
| `service.ts`（Service 基类/invoke/resolveConfig） | `service.rs`（Service trait/service_plugin） | 挂载模式保真；invoke/配置合并未移植 |
| `utils.ts`（symbols/tracker/shadow） | ——（Rust 用类型与所有权表达） | 语言机制替代 |
| `cosmokit`/`schemastery` 等 vendor | `thiserror`/`tokio`/std | 生态等价物 |

## 八、给后来者的一句话

判断一个 Cordis 移植（或任何插件框架）是否抓住了本质，看三件事：
**卸载是否精确逆序回滚**（树即事务）、**是否没有人手写启动顺序**
（依赖即图）、**框架自身能否被当成插件对待**（无内核）。API 的形状
会随宿主语言改变，这三条不会。
