package irq

import "errors"

var (
	// ErrLineNotFound 表示操作引用了未注册的中断线。
	ErrLineNotFound = errors.New("irq: line not found")
	// ErrLineExists 表示中断线重复注册。
	ErrLineExists = errors.New("irq: line already exists")
	// ErrHandlerExists 表示例程在同一线上重复注册。
	ErrHandlerExists = errors.New("irq: handler already registered")
	// ErrNotRunning 表示分发器尚未启动。
	ErrNotRunning = errors.New("irq: dispatcher not running")
	// ErrReplayTimeout 表示重放后系统未在限定时间内恢复静默。
	ErrReplayTimeout = errors.New("irq: replay did not settle")
	// ErrNoJournal 表示操作依赖的事件日志未配置。
	ErrNoJournal = errors.New("irq: journal not configured")
)
