package irq

// Device is an in-process virtual device attached to one interrupt line.
type Device struct {
	id         SourceID
	line       LineID
	mode       TriggerMode
	dispatcher *Dispatcher
}

// NewDevice creates a virtual device attached to line using mode semantics.
func NewDevice(id SourceID, line LineID, mode TriggerMode) *Device {
	return &Device{id: id, line: line, mode: mode}
}

// ID returns the device source identifier.
func (d *Device) ID() SourceID {
	return d.id
}

// attach binds the device to the dispatcher that owns its line.
func (d *Device) attach(dp *Dispatcher) {
	d.dispatcher = dp
}

// Raise emits an edge pulse or asserts the request level.
func (d *Device) Raise() {
	if d.dispatcher != nil {
		d.dispatcher.Raise(d.line, d.id)
	}
}

// Release deasserts the request level (level-triggered only).
func (d *Device) Release() {
	if d.dispatcher != nil {
		d.dispatcher.Release(d.line, d.id)
	}
}

// Line reports the line the device is attached to.
func (d *Device) Line() LineID { return d.line }

// Mode reports the device's trigger semantics.
func (d *Device) Mode() TriggerMode { return d.mode }
