package river

type Handler interface {
	OnEvent(event *EventData) error
	OnAlert(msg *StatusMsg) error
	OnClose(river *River) // OnEvent、OnAlert 引发的error会触发OnClose
}

type NopCloserAlerter func(event *EventData) error

func (f NopCloserAlerter) OnAlert(msg *StatusMsg) error   { return nil }
func (f NopCloserAlerter) OnClose(river *River)           { return }
func (f NopCloserAlerter) OnEvent(event *EventData) error { return f(event) }

type NopCloser func(event *EventData) error

func (f NopCloser) OnClose(river *River) { return }

type NopAlerter func(msg *StatusMsg) error

func (f NopAlerter) OnAlert(msg *StatusMsg) error { return nil }
