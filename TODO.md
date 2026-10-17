# TODO

> 项目遗留问题清单。按"离 Cordis 本质的距离"排序；每条注明阻碍，
> 避免下次调研重新推导。背景见 `docs/cordis-essence.md`。

## 发布备忘

- [x] **crates.io 包名撞车已解决**（2026-10-17）
  crates.io 的 `cordis-rust` 是另一项目（redoop/cordis-rust，2026-08
  发布 v0.2.1），与本仓库无关。解法：**v0.2.1 起包名改为
  `cordis-harness`**（crates.io 可用；`cordis`/`cordis-core`/
  `cordis-kernel` 均已被占）。库名仍为 `cordis`，`use cordis::` 不变；
  GitHub 仓名 cordis-rust 保持不变。

## 架构债（"无特权内核"的最后拼图）

- [ ] **typed 服务键（双轨）**
  `TypeId` 主键消 typo + `trait ServiceKey { const NAME }` 保诊断/
  未来配置层寻址。当前纯字符串键的收益（配置文件可寻址）因配置层
  缺失而归零，typo 成本全在。参考 Bevy Resources 路线。
  改动面：`AppInner.services` 键、`provide`/`get`/`expect`、`InjectDecl`。

## 语义弱化（已记录、待条件成熟再补）

- [ ] **provide 撤销不等待下游 settle**
  原版 provide 的 disposer 会 await 依赖方 fiber 收尾后再清自身存储；
  同步 Cleanup 做不到 → 服务撤销瞬间依赖方可能还有一小段在跑。
  补法：等 Rust async Drop 落地，或把 provide 的清理升级为
  spawn 一个等待任务后再删存储（需处理与二次 provide 的竞态）。

- [ ] **fiber 依赖唤醒过宽**
  `notify_service_change` 唤醒所有注入该服务名的 fiber 各自重估，
  原版按作用域过滤（`reflect.notify` 的 filter 参数）。正确性等价，
  多算几次依赖解析；服务数大后才值得收窄。

- [ ] **emit 时序弱于原版**
  原版 emit 同步执行监听器的同步部分（只是不 await promise）；
  Rust 版整体 spawn，连同步前缀都异步。对依赖 emit 顺序的观察者
  （日志流水线）有影响。备选：提供 `emit_ordered`（= 无错误的
  parallel 语义，按序 await）。

- [ ] **inject 强制性降级为约定**
  原版未声明 inject 读服务在 proxy 层报错；Rust `get` 自由读取。
  若要恢复护栏：debug 构建下 get 未命中时打 warn（携带读取方 fiber
  名与服务名），零成本近似原版报错。

## 功能缺口（依赖架构债与配置接缝）

- [ ] **配置接缝**（前置：typed 键）
  描述符/工厂形态插件 + `fiber.update(Value)` + `internal/update`
  瀑布 + `InjectDecl` 带 intercept 配置。补齐后 loader/patch/HMR
  可在不碰核心的前提下长出来。代价：`Plugin` trait 一次 breaking
  change（身份/状态拆分——当前"插件=实例"使同一实例挂两次无法
  各持一份配置）。

- [ ] **proc-macro 装饰器**
  `#[inject("database")]` / `derive(Plugin)`，对应原版 `@Inject()`。
  需新开 `cordis-macros` crate。

## 防回归备忘（实现期踩过、已修复）

- [x] **`internal/get` / `internal/set` 拦截瀑布**（2026-10 已实现）
  方案拍板：**同步瀑布监听形态**（`ListenerKind::SyncWaterfall` +
  `on_waterfall_sync`）。关键考古结论：原版 waterfall 本就是同步的
  （`events.ts` 的 `waterfall()` 无 await，挂在 Proxy trap 里也不可能
  异步），"Rust get 同步承载不了瀑布"的认知是反的。生态证据：整个
  dsh 200+ 包无任何插件监听 internal/get/set——扩展点存在但未被使用。
  保真约束：依赖就绪检查绕过读拦截（原版 `_checkImpl` 直查 store）；
  internal/set 环绕 `provide_arc`，终态 (name, value) 才是写入与撤销
  的键（disposer 捕获终态键）。
  **防死锁铁律**：同步瀑布的监听器执行绝不持任何锁——快照释放
  records 锁后才走链，链内嵌套 `get`/`provide` 安全。

- [x] **provide 直供唤醒缺失**（2026-10 已修复，勿回退）
  根上下文直接 `provide` 此前只广播 `internal/service`、不调
  `notify_service_change`（唤醒只发生在插件 fiber 转 Active 的
  `notify_provided` 路径）——与原版 provide 在 ACTIVE 态的
  `notify([name])` 不符。已补：`provide_arc` Active 分支同时唤醒。
  由 `check_predicate_gates_dependency_readiness` 测试锚定。

- [x] **check 谓词 / set 原位覆盖 / internal/dispatch**（2026-10 已实现）
  对照原版源码全量审计后补齐的三项：`provide_with_check`（谓词只参与
  依赖就绪、panic 按未就绪记日志）、`set`（仅提供方可写、不重新广播）、
  `internal/dispatch` 观测事件（公开事件分发前触发，`internal/*` 豁免
  防递归；不携带参数列表——类型化总线的有意简化）。

- [x] **once 双触发**（已修复，勿回退）
  once 的自移除必须发生在**派发快照阶段**（`snapshot` 内同步摘除），
  不能等监听器异步执行时再移——连续两次 emit 的快照都包含它，
  会双触发。见 `src/events.rs` `snapshot`。

- [x] **Fn 闭包插件不能 move 出捕获值**（写法约定）
  `fn_plugin` 的 body 是 `Fn`（可多次调用），`async move` 块会把
  捕获的 Arc move 出闭包导致编译错。正确形态：
  `move |ctx| { let x = arc.clone(); async move { … } }`。
  见 `tests/lifecycle.rs` 各处。

- [ ] **waterfall/serial 的 `global` 选项已存字段但未接线**
  `ListenOptions.global` 目前只影响 emit 系过滤派发（与原版一致：
  过滤只作用于带 filter 的派发点）；若未来给普通事件加作用域过滤，
  注意别把 global 语义搞混（豁免过滤 ≠ 跨 isolate 订阅服务）。
