package irq

import "time"

// FaultPolicy 描述异常例程的自动停用与恢复策略。
type FaultPolicy struct {
	Timeout     time.Duration
	Threshold   int
	AutoDisable bool
	Cooldown    time.Duration
	MaxRestarts int
}

// FaultRecord 描述一次可观测的例程故障。
type FaultRecord struct {
	At      time.Time
	Line    LineID
	Handler SourceID
	Reason  string
}

// handlerFaultState 跟踪单个例程的故障计数、停用与恢复状态。
type handlerFaultState struct {
	faults     int
	disabled   bool
	disabledAt time.Time
	restarts   int
}

// recordFailure 记录一次故障。返回 true 表示按策略应自动停用。
func (f *handlerFaultState) recordFailure(policy FaultPolicy, now time.Time) bool {
	f.faults++
	if !policy.AutoDisable || policy.Threshold <= 0 {
		return false
	}
	if f.faults < policy.Threshold {
		return false
	}
	f.disabled = true
	f.disabledAt = now
	return true
}

// available 报告例程当前是否可以参与询问。recovered 为 true 表示本次调用
// 恰好完成了一次冷却后的自动恢复。
func (f *handlerFaultState) available(policy FaultPolicy, now time.Time) (available bool, recovered bool) {
	if !f.disabled {
		return true, false
	}
	if policy.Cooldown <= 0 {
		return false, false
	}
	if now.Sub(f.disabledAt) < policy.Cooldown {
		return false, false
	}
	if policy.MaxRestarts > 0 && f.restarts >= policy.MaxRestarts {
		return false, false
	}
	f.disabled = false
	f.faults = 0
	f.restarts++
	return true, true
}

// recordSuccess 在例程成功认领后清除连续故障计数。
func (f *handlerFaultState) recordSuccess() {
	if !f.disabled {
		f.faults = 0
	}
}

// disable / enable 支持手动干预。
func (f *handlerFaultState) disable(now time.Time) {
	f.disabled = true
	f.disabledAt = now
}

func (f *handlerFaultState) enable() {
	f.disabled = false
	f.faults = 0
}

func (f *handlerFaultState) isDisabled() bool { return f.disabled }
