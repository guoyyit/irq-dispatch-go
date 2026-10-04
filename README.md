# irq-dispatch-go

进程内的本地虚拟设备中断分发子系统。多路虚拟设备共享数量有限的中断线、
异步产生中断，由单一分发器把中断可靠地投递到对应处理例程，并提供异常隔离、
风暴限速、确定性重放与运行指标。

## 快速开始

```go
d := irq.New(irq.Config{
    Fault: irq.FaultPolicy{Timeout: 50 * time.Millisecond, AutoDisable: true, Threshold: 3, Cooldown: time.Second},
    Storm: irq.StormConfig{Window: 100 * time.Millisecond, Threshold: 20, Merge: true, MaxBacklog: 64},
})
d.AddLine(irq.LineConfig{ID: "net", Mode: irq.EdgeTriggered, Priority: 2})
d.Register("net", irq.Handler{
    ID: "rx",
    Fn: func(ctx context.Context, env *irq.Env) (irq.Verdict, error) {
        // env.Source() 标识本次中断来源
        return irq.Claimed, nil // 或 irq.NotMine
    },
})

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
d.Start(ctx)

dev := irq.NewVirtualDevice(d, "eth0")
dev.Raise("net")
d.WaitQuiet(time.Second)
```

## 中断模型

### 边缘触发（EdgeTriggered）

每个来源维护一个三态相位：`idle → pending → active →（rearmed）`。

- 响应前多次出现的边缘合并为一次待处理，存在性不丢失。
- 例程执行期间再次到达会记录为一次“滞留（rearmed）”，例程返回后补投恰好一次。
- “已派发但 goroutine 尚未开始”的窗口由独立的 inflight 标记保护，
  该窗口内到达的边缘同样合并，不会重复或丢失。

### 电平触发（LevelTriggered）

- 请求保持期间只上报一次（reported 锁存）。
- `Deassert` 撤销后清除 asserted/reported，之后可再次上报。

## 共享中断线与多例程

一条线可注册多个 `Handler`，按注册顺序逐一询问：

- 例程返回 `NotMine` 不阻断后续例程，继续询问直到有例程 `Claimed` 或全部拒绝。
- 全部拒绝时本次投递正常结束（轨迹记为 `complete/no-owner`），不产生活锁。

## 优先级与嵌套屏蔽

优先级数值越大越高。

- 高优先级中断可在低优先级例程的屏蔽窗口内被立即调度（抢占）。
- 同优先级或更低优先级在所有在途屏蔽退出后补投。
- 补投顺序：优先级降序；同优先级按输入全局序号升序（先到先投）。
- 同一中断线一次只投递一个来源（同线串行），不同线可并发。
- 补投不丢失、不重复；每个被接收的输入最终恰好产生一次投递结果。

## 异常例程隔离

由 `FaultPolicy` 控制：

- 每个例程在独立 goroutine 中执行：panic 在该 goroutine 内 recover，
  超时后分发器不再等待，迟到的结果被丢弃，卡住的 goroutine 与主循环隔离。
- 故障例程视为 `NotMine`，继续询问同线后续例程，不波及其他例程。
- 故障通过 `Dispatcher.Faults()` 与轨迹中的 `fault/disabled/recovered` 事件可观测。
- `Threshold + AutoDisable`：连续故障达阈值自动停用；
  `Cooldown`：冷却后自动恢复（受 `MaxRestarts` 限制）。

## 风暴识别与限速

由 `StormConfig` 控制，按“线 + 来源”独立统计观测窗口内的突发：

- `Window/Threshold`：窗口内超过阈值即进入风暴。
- `Merge=true`：突发合并投递，存在性不丢，风暴结束后最终状态仍投递。
- `Merge=false` 且配置 `CoalesceDelay`：按最小间隔限速补投。
- `MaxBacklog`：每个来源的未投递总量（待投递触发状态 + 限速积压）严格有界，
  超限的输入转为合并，任意时刻积压深度不超过该上限。
- 限速脉冲只计入风暴积压，不重复登记触发状态；放行时按来源独立转移到
  线状态并入队，同线其他来源的待处理状态不会挡住本来源的补投，
  例程阻塞或同线串行派发也不会造成待处理条目重复累积。
- 电平撤销（`Deassert`）会同时取消该来源尚未放行的限速积压。
- 风暴积压计入 backlog 指标并被 `WaitQuiet` 等待，风暴结束后清零。

### 限速与共享中断线的交互

共享线上的例程执行中或阻塞时，同线其他来源的限速补投照常进行：

- 放行严格按“线 + 来源”独立结算，互不阻塞、互不跳过；每个来源的
  最终中断请求在风暴结束后恰好投递一次。
- 同线串行派发只推迟投递时点，不产生重复入队或丢失。
- 例程返回后，剩余待投递来源在有限时间内全部完成投递，系统收敛。

### 积压边界与静默判定

- backlog 指标 = 各线未投递触发状态（待投递边缘、投递期间滞留的补投脉冲、
  已置位未上报的电平）+ 各来源限速积压，如实反映全部未投递工作。
- `WaitQuiet` 只有在所有输入完成投递、且不存在任何未投递的边缘或电平
  状态时才报告静默；系统静默时 backlog 必然为零。

## 确定性顺序与重放

- 所有 `Raise/Deassert/Barrier` 在提交临界区内分配全局序号并入队，
  并发触发也具有唯一的全序输入序列。
- `Journal` 同时记录输入序列（`Inputs`）与分发轨迹（`Trace`）。
- `Journal.Replay(builder)` 用相同的线/例程注册重建分发器，逐个回放输入，
  返回重放轨迹。
- 需要逐字节一致的分发轨迹时，使用 `Config{InlineDispatch: true}`：
  例程在分发主循环内同步执行，消除 goroutine 调度引入的时序交错。
- `Barrier()` 在输入序列中标记确定性边界并阻塞等待主循环确认：
  普通模式下保证屏障前的输入已被接收并做过一次派发决策；
  `BarrierGated` 模式下整批只在屏障点统一派发，是并发轨迹确定化的关键。
- 一般可使用 `DispatchDecisions` + `DecisionsEqual` 比较“每个输入最终被谁认领”，
  它对执行时序不敏感；`TracesEqual` 比较完整轨迹（忽略绝对时间戳）。

## 运行指标

`Dispatcher.Metrics()` 随时返回 `MetricsSnapshot`，热路径全部使用原子计数，
采集不阻塞分发：

- 计数：raised / delivered / claimed / refused / faults / stormMerged / deferred。
- 积压：pending 通知 + 风暴限速积压。
- 延迟：min / max / mean 及分桶分布（`DefaultLatencyBuckets` 可替换）。
- 吞吐：自启动以来 delivered/秒。

## 配置项

| 配置 | 说明 |
| --- | --- |
| `Clock` | 时间源，默认墙上时钟；可注入 `ManualClock` 做逻辑时钟测试。 |
| `Journal` | 事件日志，默认自动创建。 |
| `Fault` | 超时、故障阈值、自动停用、冷却与最大恢复次数。 |
| `Storm` | 观测窗口、阈值、合并/限速、限速间隔、最大积压。 |
| `MaxConcurrent` | 同时在途例程上限（默认 64）。 |
| `InboxSize` | 输入环形缓冲容量（默认 4096），满时提交方反压。 |
| `InlineDispatch` | 同步内联执行，用于确定性测试与重放。 |
| `BarrierGated` | 普通输入只入待处理，到 `Barrier()` 点才整批统一派发，配合内联模式把并发轨迹完全确定化。 |

## 测试

`go test ./...`（`-race` 已验证）覆盖：

- 边缘合并不丢失 / 电平保持只上报一次与撤销清除；
- 共享线逐例程询问直到认领或全部拒绝；
- 高优先级抢占与屏蔽退出后的有序补投；
- panic/超时隔离、自动停用与冷却恢复；
- 风暴合并、限速补投、有界积压与最终状态不丢；
- 共享线 + 阻塞例程叠加限速：积压有界、静默判定准确、返回后有限时间收敛；
- 同线多来源同时限速：补投相互独立、不跳过、不重复；
- 风暴结束后的最终中断请求恰好投递一次；
- 限速风暴下的确定性重放（`InlineDispatch` + 逻辑时钟）；
- 多设备并发触发的确定性重放；
- 指标查询。

新增场景位于 `storm_shared_test.go`，均使用通道/屏障同步并发时序，
可用 `go test -race -run 'TestSharedLine|TestStorm|TestRateLimited' ./...`
单独验证。
