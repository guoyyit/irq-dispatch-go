package irq

// VirtualDevice 是进程内模拟的中断来源设备。
type VirtualDevice struct {
	id         SourceID
	dispatcher *Dispatcher
}

// NewVirtualDevice 在分发器上创建一个命名的虚拟设备。
func NewVirtualDevice(d *Dispatcher, id SourceID) *VirtualDevice {
	return &VirtualDevice{id: id, dispatcher: d}
}

// ID 返回设备标识。
func (v *VirtualDevice) ID() SourceID { return v.id }

// Raise 产生一次中断（边缘：一次脉冲；电平：请求保持）。
func (v *VirtualDevice) Raise(line LineID) error {
	return v.dispatcher.Raise(line, v.id)
}

// Deassert 撤销电平请求。
func (v *VirtualDevice) Deassert(line LineID) error {
	return v.dispatcher.Deassert(line, v.id)
}
