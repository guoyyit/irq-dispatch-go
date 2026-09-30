// Package irq 实现进程内的本地虚拟设备中断分发子系统。
//
// 详见随包提供的 README.md，这里给出最常用的入口：
//
//   - New 构造分发器（Config 配置故障/风暴/并发/时钟/日志）。
//   - Dispatcher.AddLine 与 Dispatcher.Register 建立中断线与处理例程。
//   - Dispatcher.Raise/Deassert 或 VirtualDevice 产生中断输入。
//   - Dispatcher.Metrics 查询延迟分布、吞吐与积压。
//   - Journal.Replay 按记录的输入序列确定性重放并比对轨迹。
package irq
