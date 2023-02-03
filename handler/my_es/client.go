package my_es

// BulkRequest is used to send multi request in batch.
type BulkRequest struct {
	Action   string
	Index    string
	ID       string
	Parent   string
	Pipeline string

	Data map[string]interface{}
}
