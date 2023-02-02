package river

type EventHandler interface {
	Handle(event *EventData) error
}

type HandlerFunc func(event *EventData) error

func (f HandlerFunc) Handle(event *EventData) error {
	return f(event)
}
