package river

type Handler interface {
	OnEvent(event *EventData) error
	OnAlert(msg *StatusMsg) error
	OnClose(river *River) // OnEvent、OnAlert抛出的error会触发OnClose
}

type NopCloserAlerter func(event *EventData) error

func (f NopCloserAlerter) OnAlert(*StatusMsg) error       { return nil }
func (f NopCloserAlerter) OnClose(*River)                 { return }
func (f NopCloserAlerter) OnEvent(event *EventData) error { return f(event) }

type NopCloser func(event *EventData) error

func (f NopCloser) OnClose(*River) { return }

type NopAlerter func(msg *StatusMsg) error

func (f NopAlerter) OnAlert(*StatusMsg) error { return nil }
