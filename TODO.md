# TODO

> 项目遗留问题清单。按"离 Cordis 本质的距离"排序；每条注明阻碍，
> 避免下次调研重新推导。背景见 `docs/cordis-essence.md`。

## 架构债（"无特权内核"的最后拼图）

- [ ] **`internal/get` / `internal/set` 拦截瀑布**
  原版服务读写可被拦截/替换（shadow、fallback 都靠它），是框架自举
  的最后一块。阻碍：Rust `get` 是同步快速路径，承载不了异步瀑布；
  需要先决定走 **async resolve API**（`ctx.resolve::<T>(name).await`
  流经瀑布，`get` 保留为快速路径）还是 **同步瀑布监听形态**
  （新增第三种 ListenerKind）。定了方案再动。
  位置：`src/context.rs` `resolve_service_impl` / `expect`。

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

## 功能缺口（依赖前两项架构债）

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
