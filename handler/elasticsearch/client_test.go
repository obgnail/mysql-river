package elasticsearch

import (
	"testing"
)

var client = newClient()

func newClient() *Client {
	cfg := new(ClientConfig)
	cfg.Addr = "127.0.0.1:9200"
	cfg.User = ""
	cfg.Password = ""
	return NewClient(cfg)
}

func makeTestData(arg1 string, arg2 string) map[string]interface{} {
	m := make(map[string]interface{})
	m["name"] = arg1
	m["content"] = arg2

	return m
}

func TestClient_CreateMapping(t *testing.T) {
	index := "dummy"
	mapping := map[string]interface{}{
		"river_health": map[string]interface{}{
			"_parent": map[string]string{"type": "river_health_parent"},
		},
	}

	err := client.CreateMapping(index, mapping)
	t.Log(err)
}

// ok
func TestClient_GetMapping(t *testing.T) {
	index := "test2"
	result, err := client.GetMapping(index)
	t.Log(result, err)
}

// ok
func TestClient_DeleteIndex(t *testing.T) {
	err := client.DeleteIndex("test2")
	t.Log(err)
}

// ok
func TestClient_Get(t *testing.T) {
	result, err := client.Get("test2", "01")
	t.Log(result, err)
}

// ok
func TestClient_Update(t *testing.T) {
	data := map[string]interface{}{
		"id":   1,
		"name": "lihua2",
		"text": "qwe",
	}
	err := client.Update("test2", "01", data)
	t.Log(err)
}

// ok
func TestClient_Exists(t *testing.T) {
	res, err := client.Exists("test2", "101")
	t.Log(res, err)
}

// ok
func TestClient_Delete(t *testing.T) {
	err := client.Delete("test2", "01")
	t.Log(err)
}

// ok
func TestClient_Bulk(t *testing.T) {
	items := []*BulkRequest{
		{Action: "create", Index: "test2", ID: "03", Parent: "", Pipeline: "", Data: map[string]interface{}{"name": "name3", "id": 3, "text": "qqqqqqq"}},
		{Action: "update", Index: "test2", ID: "02", Parent: "", Pipeline: "", Data: map[string]interface{}{"name": "name22222222222"}},
	}
	resp, err := client.Bulk(items)
	t.Log(resp, err)
}
