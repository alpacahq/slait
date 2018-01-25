package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alpacahq/slait/cache"
	"github.com/alpacahq/slait/socket"
	"github.com/kataras/iris"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type RESTTestSuite struct{}

var _ = Suite(&RESTTestSuite{})

func pullFromCache() {
	for {
		select {
		case <-cache.Pull():
			continue
		case <-cache.PullAdditions():
			continue
		case <-cache.PullRemovals():
			continue
		default:
			continue
		}
	}
}

func (s *RESTTestSuite) TestCache(c *C) {
	cache.Build(c.MkDir())
	go pullFromCache()

	app := iris.New()
	app.HandleMany("GET POST DELETE", "/topics", TopicsHandler)
	app.HandleMany("GET PUT DELETE", "/topics/{topic:string}", TopicHandler)
	app.HandleMany("GET PUT DELETE", "/topics/{topic:string}/{partition:string}", PartitionHandler)
	app.Any("/ws", iris.FromStd(socket.GetHandler().Serve))
	app.Build()

	// create some topics
	tr := TopicsRequest{Topic: "bars", Partitions: []string{"NVDA_composite"}}
	data, _ := json.Marshal(tr)
	req, _ := http.NewRequest("POST", "/topics", bytes.NewBuffer(data))
	rr := httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)

	tr = TopicsRequest{Topic: "quotes", Partitions: []string{"AMD_bats"}}
	data, _ = json.Marshal(tr)
	req, _ = http.NewRequest("POST", "/topics", bytes.NewBuffer(data))
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)

	// list topics
	req, _ = http.NewRequest("GET", "/topics", nil)
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	topics := []string{}
	json.Unmarshal(rr.Body.Bytes(), &topics)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)
	c.Assert(len(topics), Equals, 2)

	// append some data
	pr := PartitionRequestResponse{
		Data: cache.GenData(),
	}
	data, _ = json.Marshal(pr)
	req, _ = http.NewRequest("PUT", "/topics/bars/NVDA_composite", bytes.NewBuffer(data))
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)

	pr = PartitionRequestResponse{
		Data: cache.GenData(),
	}
	data, _ = json.Marshal(pr)
	req, _ = http.NewRequest("PUT", "/topics/quotes/AMD_bats", bytes.NewBuffer(data))
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)

	// get a topic
	req, _ = http.NewRequest("GET", "/topics/bars", nil)
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	tResp := []string{}
	json.Unmarshal(rr.Body.Bytes(), &tResp)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)
	c.Assert(len(tResp), Equals, 1)

	// get a partition
	req, _ = http.NewRequest("GET", "/topics/bars/NVDA_composite", nil)
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	pResp := PartitionRequestResponse{}
	json.Unmarshal(rr.Body.Bytes(), &pResp)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)
	c.Assert(len(pResp.Data), Equals, 5)

	// delete a partition
	req, _ = http.NewRequest("DELETE", "/topics/bars/NVDA_composite", nil)
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)

	// delete a topic
	data, _ = json.Marshal(pr)
	req, _ = http.NewRequest("DELETE", "/topics/bars", bytes.NewBuffer(data))
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)

	// delete all topics
	req, _ = http.NewRequest("DELETE", "/topics", bytes.NewBuffer(data))
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusOK)
}

func (s *RESTTestSuite) TestErrors(c *C) {
	app := iris.New()
	app.HandleMany("GET POST DELETE", "/topics", TopicsHandler)
	app.HandleMany("GET PUT DELETE", "/topics/{topic:string}", TopicHandler)
	app.HandleMany("GET PUT DELETE", "/topics/{topic:string}/{partition:string}", PartitionHandler)
	app.Any("/ws", iris.FromStd(socket.GetHandler().Serve))
	app.Build()

	// POST topic [empty topic]
	tr := TopicsRequest{
		Partitions: []string{"NVDA_composite"},
	}
	data, _ := json.Marshal(tr)
	req, _ := http.NewRequest("POST", "/topics", bytes.NewBuffer(data))
	rr := httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusBadRequest)

	// POST topic [topic exists]
	tr = TopicsRequest{
		Topic:      "bars",
		Partitions: []string{"NVDA_composite"},
	}
	data, _ = json.Marshal(tr)
	req, _ = http.NewRequest("POST", "/topics", bytes.NewBuffer(data))
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	req, _ = http.NewRequest("POST", "/topics", bytes.NewBuffer(data))
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusBadRequest)

	// GET partition [bad from timestamp]
	req, _ = http.NewRequest("GET", "/topics/bars/NVDA_composite?from=badtimestamp", nil)
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusBadRequest)

	// GET partition [bad to timestamp]
	req, _ = http.NewRequest("GET", "/topics/bars/NVDA_composite?from=2006-01-02T15:04:05-07:00&to=badtimestamp", nil)
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusBadRequest)

	// PUT to partition [nil data]
	req, _ = http.NewRequest("PUT", "/topics/bars/NVDA_composite", nil)
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusBadRequest)

	// PUT to partition [empty data]
	pr := PartitionRequestResponse{Data: cache.Entries{}}
	data, _ = json.Marshal(pr)
	req, _ = http.NewRequest("PUT", "/topics/bars/NVDA_composite", bytes.NewBuffer(data))
	rr = httptest.NewRecorder()
	app.ServeHTTP(rr, req)
	c.Assert(rr.Result().StatusCode, Equals, iris.StatusBadRequest)
}
