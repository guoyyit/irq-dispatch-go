package irq

import (
	"context"
	"time"
)

// LineID 标识一条中断线。
type LineID string

// SourceID 标识一个中断来源（虚拟设备/事件源）。
type SourceID string

// Priority 为中断与例程的优先级，数值越大优先级越高。
type Priority int

// TriggerMode 描述中断线的触发语义。
type TriggerMode int

const (
	// EdgeTriggered 边缘触发：响应前的多次上报合并为一次待处理，存在性不丢失。
	EdgeTriggered TriggerMode = iota
	// LevelTriggered 电平触发：请求保持期间只上报一次，撤销后状态清除。
	LevelTriggered
)

// OpKind 记录输入事件的种类。
type OpKind int

const (
	OpRaise OpKind = iota
	OpDeassert
	OpBarrier
)

func (op OpKind) String() string {
	switch op {
	case OpDeassert:
		return "deassert"
	case OpBarrier:
		return "barrier"
	default:
		return "raise"
	}
}

// Verdict 为处理例程对一次询问的回应。
type Verdict int

const (
	// NotMine 声明中断不归属本处理例程，分发器继续询问下一例程。
	NotMine Verdict = iota
	// Claimed 例程认领并处理了本次中断。
	Claimed
)

// HandlerFunc 是中断处理例程的签名。
// 返回 Verdict 表示是否认领；返回 error 表示例程发生故障，不得波及其他例程。
type HandlerFunc func(ctx context.Context, env *Env) (Verdict, error)

// Handler 描述一条中断线上注册的处理例程。
type Handler struct {
	ID       SourceID
	Priority Priority
	Fn       HandlerFunc
}

// Env 传递给处理例程的运行环境。
type Env struct {
	dispatcher *Dispatcher
	line       LineID
	source     SourceID
	priority   Priority
	raisedAt   time.Time
}

// Line 返回当前正在处理的中断线。
func (e *Env) Line() LineID { return e.line }

// Source 返回被认领来源的标识。
func (e *Env) Source() SourceID { return e.source }

// Priority 返回当前处理优先级。
func (e *Env) Priority() Priority { return e.priority }

// RaisedAt 返回中断被记录的时间。
func (e *Env) RaisedAt() time.Time { return e.raisedAt }

// LineConfig 描述一条中断线的静态配置。
type LineConfig struct {
	ID       LineID
	Mode     TriggerMode
	Priority Priority
}
