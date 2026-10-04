package irq

import "time"

// StormConfig 描述中断风暴识别与限速策略。
type StormConfig struct {
	Window        time.Duration
	Threshold     int
	Merge         bool
	CoalesceDelay time.Duration
	MaxBacklog    int
}

// StormDecision 是单次中断输入在风暴控制器下得到的处置。
type StormDecision int

const (
	// StormAdmit 正常投递。
	StormAdmit StormDecision = iota
	// StormMerge 处于风暴中，本次输入合并进既有待处理状态。
	StormMerge
	// StormDelay 处于限速中，本次输入进入有界积压等待补投。
	StormDelay
)

// stormState 维护单个来源在观测窗口内的突发计数与积压深度。
type stormState struct {
	cfg        StormConfig
	window     []time.Time
	backlog    int
	storm      bool
	admitted   int
	suppressed int
	timerArmed bool
}

func newStormState(cfg StormConfig) *stormState {
	return &stormState{cfg: cfg}
}

func (s *stormState) prune(now time.Time) {
	if s.cfg.Window <= 0 || len(s.window) == 0 {
		s.window = s.window[:0]
		return
	}
	cutoff := now.Add(-s.cfg.Window)
	drop := 0
	for drop < len(s.window) && !s.window[drop].After(cutoff) {
		drop++
	}
	if drop > 0 {
		s.window = append(s.window[:0], s.window[drop:]...)
	}
}

// observe 记录一次输入并给出处置决策。合并与限速的存在性均不丢失。
// queued 表示该来源当前已有待处理通知在队：通知与限速积压共同构成
// 该来源的未投递积压，二者合计不得突破 MaxBacklog，保证积压指标严格有界。
func (s *stormState) observe(now time.Time, queued bool) StormDecision {
	s.prune(now)
	s.window = append(s.window, now)

	if !s.enabled() {
		s.admitted++
		return StormAdmit
	}

	if !s.storm && len(s.window) <= s.cfg.Threshold {
		s.admitted++
		return StormAdmit
	}
	s.storm = true

	if s.cfg.Merge || s.cfg.CoalesceDelay <= 0 {
		s.suppressed++
		return StormMerge
	}
	held := s.backlog
	if queued {
		held++
	}
	if held >= s.cfg.MaxBacklog {
		s.suppressed++
		return StormMerge
	}
	s.backlog++
	return StormDelay
}

// release 在限速间隔到达后放行一个积压输入，返回是否仍有积压需要继续定时。
func (s *stormState) release() (remaining int) {
	if s.backlog > 0 {
		s.backlog--
		s.admitted++
	}
	s.timerArmed = false
	return s.backlog
}

// settled 在窗口内无新突发且积压清空后退出风暴状态。
func (s *stormState) settled(now time.Time) bool {
	if !s.storm {
		return true
	}
	s.prune(now)
	if len(s.window) > s.cfg.Threshold || s.backlog > 0 {
		return false
	}
	s.storm = false
	return true
}

func (s *stormState) enabled() bool {
	return s.cfg.Window > 0 && s.cfg.Threshold > 0
}

// inStorm 报告当前是否处于风暴状态。
func (s *stormState) inStorm() bool { return s.storm }

// depth 返回当前有界积压深度。
func (s *stormState) depth() int { return s.backlog }
