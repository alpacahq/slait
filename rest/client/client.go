package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/alpacahq/slait/rest"
	"github.com/valyala/fasthttp"
)

type SlaitClient struct {
	client   *fasthttp.Client
	Endpoint string
}

// used for setting up the structure
func (sc *SlaitClient) PostTopic(tr rest.TopicsRequest) (err error) {
	data, err := json.Marshal(tr)
	if err != nil {
		return err
	}
	_, err = sc.request("POST", sc.Endpoint+"/topics", data)
	return err
}

// used for delivering the data
func (sc *SlaitClient) PutPartition(topic, partition string, data []byte) error {
	_, err := sc.request(
		"PUT",
		fmt.Sprintf("%v/topics/%v/%v", sc.Endpoint, topic, partition),
		data,
	)
	return err
}

// delete a partition
func (sc *SlaitClient) DeletePartition(topic, partition string) error {
	_, err := sc.request(
		"DELETE",
		fmt.Sprintf("%v/topics/%v/%v", sc.Endpoint, topic, partition),
		nil,
	)
	return err
}

func (sc *SlaitClient) GetPartition(topic, partition string, from, to *time.Time, last int) (*rest.PartitionRequestResponse, error) {
	q := "&"
	if from != nil && !from.IsZero() {
		q = fmt.Sprintf("%v&%v=%v", q, "since", from.Format(time.RFC3339))
	}
	if to != nil && !to.IsZero() {
		q = fmt.Sprintf("%v&%v=%v", q, "to", to.Format(time.RFC3339))
	}
	if last > 0 {
		q = fmt.Sprintf("%v&%v=%v", q, "last", last)
	}
	data, err := sc.request(
		"GET",
		fmt.Sprintf("%v/topics/%v/%v?%v", sc.Endpoint, topic, partition, q),
		nil)
	if err != nil {
		return nil, err
	}
	resp := rest.PartitionRequestResponse{}
	if err = json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, err
}

func (sc *SlaitClient) request(method, url string, data []byte) ([]byte, error) {
	if sc.client == nil {
		sc.client = &fasthttp.Client{}
	}
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(url)
	req.Header.SetMethod(method)
	req.SetBody(data)
	resp := fasthttp.AcquireResponse()
	err := sc.client.Do(req, resp)

	if resp.StatusCode() != http.StatusOK {
		if err == nil {
			err = errors.New(
				fmt.Sprintf(
					"Slait request failed - URL: %v - Code: %v - Response: %v",
					url,
					resp.StatusCode(),
					resp.String(),
				),
			)
		}
	}
	return resp.Body(), err
}
