# irq-dispatch-go

进程内本地虚拟设备中断子系统。多路虚拟设备共享有限的中断线，持续产生异步中断，由单一调度器可靠分发到处理例程，并提供异常隔离、风暴限速、确定性重放与运行指标。

## 快速开始

```go
cfg := irq.DefaultConfig()
dp := irq.New(cfg)

// 在共享线 1 上注册例程（按优先级降序询问）
dp.Register(1, irq.EdgeTriggered, irq.Handler{
    ID:       "net-rx",
    Priority: 5,
    Timeout:   50 * time.Millisecond, // 0 表示使用 Fault.Timeout 默认值
    Handle: func(ctx context.Context, ev irq.Event) bool {
        if ev.Source != "net0" {
            return false // 不归属，继续询问下一个例程
        }
        // ... 处理，注意响应 ctx.Done() 以支持超时取消
        return true
    },
})

dp.Start()
defer dp.Stop(ctx)

dev := irq.NewDevice("net0", 1, irq.EdgeTriggered)
dev.Raise() // 边缘脉冲；电平触发用 Raise()/Release() 表示保持/撤销
```

## 中断模型

- **边缘触发（EdgeTriggered）**：响应完成前的多个脉冲合并为一次待处理，存在性不丢失，合并数量见 `Event.Coalesced`。
- **电平触发（LevelTriggered）**：请求保持期间只上报一次；`Release()` 撤销后清除状态，再次 `Raise()` 重新上报。

## 共享线与例程询问

同一条线可注册多个例程，按 `Priority` 降序（同优先级按注册顺序）逐一询问；返回 `false` 不阻断后续例程，首个返回 `true` 的例程认领导入；全部拒绝则记录线级 rejection。

## 优先级与屏蔽窗口

例程执行期间建立与线优先级相同的屏蔽。严格更高优先级的中断可抢占并嵌套执行；同级或更低优先级进入有序待投递队列，屏蔽解除后按 FIFO 补投，每个中断恰好投递一次。

## 异常隔离

每个例程运行在独立 goroutine 中，带 panic 恢复与超时 context。panic/超时被隔离，不影响同线其它例程与分发主循环；故障写入日志与指标，并可按 `FaultPolicy` 自动停用（`DisableAfter`）与到期恢复（`RecoverAfter`）。例程应响应 `ctx.Done()` 以便超时后退出。

## 风暴识别与限速

按来源在滑动 `Window` 内计数，超过 `Threshold` 判定为风暴：后续脉冲合并投递，积压深度受 `MaxBacklog` 约束（越界抑制但保留存在性）；窗口内无新脉冲后风暴结清，并补投一个最终聚合事件，保证最终状态不丢失。

## 确定性与重放

所有分发决策在单一调度器 goroutine 上完成，输入经有序信箱串行化，完整轨迹写入 `Journal`。`Dispatcher.Inputs()` 给出输入脚本，`Replay` / `ReplayWithConfig` 用同步引擎重放，相同输入产出逐记录一致的轨迹。需要“轨迹仅由输入决定”时，关闭风暴窗口（`Storm.Threshold/Window = 0`）并使用不阻塞的例程；风暴、超时等时间行为在相同 `Config` 下重放。

## 运行指标

`Dispatcher.Metrics().Snapshot()` 返回 delivered/coalesced/suppressed/faults 计数、当前积压以及 min/avg/p50/p90/p99/max 延迟分布。写入全部为原子操作，延迟使用定长无锁环形缓冲，采集不会阻塞分发路径，可并发查询。

## 配置项

| 配置 | 含义 |
| --- | --- |
| `Fault.Timeout` | 例程默认超时 |
| `Fault.DisableAfter` | 连续多少次故障后自动停用（0 关闭） |
| `Fault.RecoverAfter` | 停用多久后自动恢复（0 表示不恢复） |
| `Storm.Window` | 风暴观测窗口 |
| `Storm.Threshold` | 窗口内突发阈值，超过即判定风暴 |
| `Storm.MaxBacklog` | 每来源有界积压深度 |

## 测试

```sh
go test -race ./...
```

覆盖：两种触发语义、共享线逐一询问、优先级嵌套与补投、风暴聚合与积压有界、panic/超时隔离与停用恢复、确定性重放，以及指标采集。
