package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	conntrack "github.com/mwitkow/go-conntrack"
	"github.com/rubyist/circuitbreaker"
	"golang.org/x/net/context/ctxhttp"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

// failureBehavior is an enum for client failure behaviors.
type failureBehavior int

// Possible failureBehavior values.
const (
	failureBehaviorAll failureBehavior = iota
	failureBehaviorAny
)

// contextKey is a value for use with context.WithValue. It's used as
// a pointer so it fits in an interface{} without allocation.
type contextKey struct {
	name string
}

func (k *contextKey) String() string { return "mixer context value " + k.name }

var (
	// FailureBehaviorContextKey is a context key. It can be used to configure
	// failure behavior of a client. The associated value will be of
	// FailureBehavior.
	failureBehaviorContextKey = &contextKey{"mixer-failure-behavior"}
)

const (
	clientBreakerThreshold = 5
)

var (
	backendQueries = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mixer",
			Name:      "backend_queries_total",
			Help:      "Total number of queries to backends.",
		},
	)
	backendQueriesFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mixer",
			Name:      "backend_queries_failed_total",
			Help:      "Total number of failed queries to backends.",
		},
		[]string{"backend"},
	)
	backendQueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mixer",
			Name:      "backend_queries_duration_seconds",
			Help:      "Duration of queries to backends.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"backend"},
	)
	backendBreakerTripped = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mixer",
			Name:      "backend_circuit_breaker_tripped",
			Help:      "Whether a circuit breaker is tripped.",
		},
		[]string{"backend"},
	)
	backendLastTripTimestampSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mixer",
			Name:      "backend_circuit_breaker_last_trip_timestamp_seconds",
			Help:      "Timestamp of the last trip of a circuit breaker.",
		},
		[]string{"backend"},
	)
	backendLastRetryTimestampSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mixer",
			Name:      "backend_circuit_breaker_last_retry_timestamp_seconds",
			Help:      "Timestamp of the last retry of a circuit breaker.",
		},
		[]string{"backend"},
	)
)

func init() {
	registerer.MustRegister(backendQueries, backendQueriesFailed, backendQueryDuration)
	registerer.MustRegister(backendBreakerTripped, backendLastTripTimestampSeconds, backendLastRetryTimestampSeconds)
}

// client is a remote read client that wraps remote.Client.
type client struct {
	client  *http.Client
	timeout time.Duration
	breaker *circuit.Breaker
	extls   model.LabelSet
	url     *url.URL
}

// newClient creates a new client.
func newClient(u *url.URL, extls model.LabelSet, timeout, dialTimeout time.Duration) *client {
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: conntrack.NewDialContextFunc(
				conntrack.DialWithTracing(),
				conntrack.DialWithName(u.String()),
				conntrack.DialWithDialer(&net.Dialer{
					Timeout:   dialTimeout,
					KeepAlive: 30 * time.Second,
				}),
			),
			DisableKeepAlives:   false,
			DisableCompression:  true,
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	c := &client{
		client:  httpClient,
		timeout: timeout,
		breaker: circuit.NewConsecutiveBreaker(clientBreakerThreshold),
		extls:   extls,
		url:     u,
	}
	backendBreakerTripped.WithLabelValues(c.String()).Set(0)
	backendQueriesFailed.WithLabelValues(c.String()).Add(0)
	go c.collectMetrics()
	return c
}

// String returns the name of the client.
func (c client) String() string {
	return c.url.String()
}

// collectMetrics collects metrics by subscribing to events from circuit breaker.
func (c *client) collectMetrics() {
	for event := range c.breaker.Subscribe() {
		switch event {
		case circuit.BreakerTripped:
			backendLastTripTimestampSeconds.WithLabelValues(c.String()).Set(float64(time.Now().Unix()))
			backendBreakerTripped.WithLabelValues(c.String()).Set(1)
		case circuit.BreakerReset:
			backendBreakerTripped.WithLabelValues(c.String()).Set(0)
		case circuit.BreakerFail:
			backendQueriesFailed.WithLabelValues(c.String()).Inc()
		case circuit.BreakerReady:
			backendLastRetryTimestampSeconds.WithLabelValues(c.String()).Set(float64(time.Now().Unix()))
			backendBreakerTripped.WithLabelValues(c.String()).Set(1)
		}
	}
}

// doRead queries samples from a remote read endpoint.
func (c *client) doRead(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			query,
		},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal read request: %v", err)
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %v", err)
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpResp, err := ctxhttp.Do(ctx, c.client, httpReq)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("server returned HTTP status %s", httpResp.Status)
	}

	compressed, err = ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response body: %v", err)
	}

	if len(resp.Results) != len(req.Queries) {
		return nil, fmt.Errorf("responses: want %d, got %d", len(req.Queries), len(resp.Results))
	}

	return resp.Results[0], nil
}

// read reads from a remote read endpoint.
func (c *client) read(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
	if !c.breaker.Ready() {
		backendQueriesFailed.WithLabelValues(c.String()).Inc()
		return nil, fmt.Errorf("backend not ready: retry in %s", c.breaker.BackOff.NextBackOff())
	}
	begin := time.Now()
	res, err := c.doRead(ctx, query)
	duration := time.Since(begin).Seconds()

	if err != nil {
		select {
		case <-ctx.Done():
			// Ignore context cancellation
			if ctx.Err() != context.Canceled {
				c.breaker.Fail()
			}
		default:
			c.breaker.Fail()
		}
		return nil, err
	}
	c.breaker.Success()
	backendQueryDuration.WithLabelValues(c.String()).Observe(duration)

	// Remove external labels from the result
	series := res.GetTimeseries()
	for i := 0; i < len(series); i++ {
		oldls := series[i].GetLabels()
		newls := make([]*prompb.Label, 0, len(oldls))
		for _, l := range oldls {
			if _, ok := c.extls[model.LabelName(l.GetName())]; !ok {
				newls = append(newls, l)
			}
		}
		series[i].Labels = newls
	}
	return res, nil
}

// multiClient is a client that reads from multiple clients.
type multiClient struct {
	clients []*client

	logger log.Logger
}

// read reads from all clients.
func (m *multiClient) read(ctx context.Context, query *prompb.Query) ([]*prompb.QueryResult, error) {
	var wg sync.WaitGroup
	out := make(chan *prompb.QueryResult)
	wg.Add(len(m.clients))
	backendQueries.Inc()
	for _, c := range m.clients {
		go func(c *client) {
			defer wg.Done()
			res, err := c.read(ctx, query)
			if err != nil {
				level.Error(m.logger).Log("msg", "error reading from backend", "backend", c, "err", err)
			} else {
				out <- res
			}
		}(c)
	}
	// Close channel after all clients return
	go func() {
		wg.Wait()
		close(out)
	}()

	// Collect results
	res := make([]*prompb.QueryResult, 0, len(m.clients))
	for r := range out {
		res = append(res, r)
	}

	// Check for errors
	var err error
	switch ctx.Value(failureBehaviorContextKey).(failureBehavior) {
	case failureBehaviorAll:
		if len(res) == 0 {
			err = errors.New("error reading from all backends")
		}
	case failureBehaviorAny:
		if len(res) != len(m.clients) {
			err = errors.New("error reading from some backends")
		}
	}
	return res, err
}
