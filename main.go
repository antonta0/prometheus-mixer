package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"
	promlogflag "github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/promql"
)

var (
	registry = prometheus.NewRegistry()
	// gatherer combines metrics from registry with a subset of
	// DefaultGatherer metrics.
	gatherer = prometheus.Gatherers{registry, prometheus.GathererFunc(filterDefaultRegistry)}

	// registerer is a Prometheus Registerer that points to metrics registry
	// used by the mixer.
	registerer prometheus.Registerer = registry
)

func init() {
	registerer.MustRegister(prometheus.NewProcessCollector(os.Getpid(), ""))
	registerer.MustRegister(prometheus.NewGoCollector())
	registerer.MustRegister(version.NewCollector("mixer"))
}

func filterDefaultRegistry() ([]*dto.MetricFamily, error) {
	ret := []*dto.MetricFamily{}
	mfs, err := prometheus.DefaultGatherer.Gather()
	for _, mf := range mfs {
		switch mf.GetName() {
		case
			"http_requests_total",
			"http_request_duration_microseconds",
			"http_request_size_bytes",
			"http_response_size_bytes":
			ret = append(ret, filterHandlers(mf))
		case
			"net_conntrack_dialer_conn_attempted_total",
			"net_conntrack_dialer_conn_closed_total",
			"net_conntrack_dialer_conn_established_total",
			"net_conntrack_dialer_conn_failed_total",
			"net_conntrack_listener_conn_accepted_total",
			"net_conntrack_listener_conn_closed_total":
			ret = append(ret, mf)
		}
	}
	return ret, err
}

func filterHandlers(mf *dto.MetricFamily) *dto.MetricFamily {
	ms := []*dto.Metric{}
	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() != "handler" {
				continue
			}
			switch lp.GetValue() {
			case
				"query",
				"query_range",
				"read",
				"prometheus":
				ms = append(ms, m)
			}
		}
	}
	return &dto.MetricFamily{
		Name:   mf.Name,
		Help:   mf.Help,
		Type:   mf.Type,
		Metric: ms,
	}
}

type labelSetValue model.LabelSet

func (ls *labelSetValue) Set(value string) error {
	if value == "" {
		return nil
	}
	parts := strings.SplitN(value, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("expected NAME=VALUE got '%s'", value)
	}
	(*ls)[model.LabelName(parts[0])] = model.LabelValue(parts[1])
	return model.LabelSet(*ls).Validate()
}

func (ls *labelSetValue) String() string {
	return (*ls).String()
}

func (ls *labelSetValue) IsCumulative() bool {
	return true
}

func run() int {
	cfg := struct {
		externalLabels   labelSetValue
		mixer            mixerOptions
		lookbackDelta    time.Duration
		queryConcurrency int
		queryTimeout     time.Duration

		logLevel promlog.AllowedLevel
	}{
		externalLabels: labelSetValue{},
	}

	a := kingpin.New(filepath.Base(os.Args[0]), "Prometheus mixer")

	a.Version(version.Print("prometheus-mixer"))

	a.HelpFlag.Short('h')

	a.Flag("mixer.backend",
		"List of remote read endpoints to query samples from. Can be specified multiple times.").
		Default("http://127.0.0.1:9090/api/v1/read").URLListVar(&cfg.mixer.Backends)

	a.Flag("mixer.backend-timeout",
		"Timeout for reading from a backend.").
		Default("1m").DurationVar(&cfg.mixer.BackendTimeout)

	a.Flag("mixer.backend-dial-timeout",
		"Timeout for connecting to a backend.").
		Default("30s").DurationVar(&cfg.mixer.BackendDialTimeout)

	a.Flag("mixer.external-label",
		"External labels in name=value format. Can be specified multiple times").
		Default("").SetValue(&cfg.externalLabels)

	a.Flag("mixer.listen-address",
		"Address to listen on for API and telemtry.").
		Default(":9900").StringVar(&cfg.mixer.ListenAddress)

	a.Flag("mixer.read-timeout",
		"Maximum duration before timing out read of the request, and closing idle connections.").
		Default("5m").DurationVar(&cfg.mixer.ReadTimeout)

	a.Flag("mixer.max-connections",
		"Maximum number of simultaneous connections.").
		Default("512").IntVar(&cfg.mixer.MaxConnections)

	a.Flag("query.lookback-delta",
		"The delta difference allowed for retrieving metrics during expression evaluations.").
		Default("5m").DurationVar(&cfg.lookbackDelta)

	a.Flag("query.timeout",
		"Maximum time a query may take before being aborted.").
		Default("2m").DurationVar(&cfg.queryTimeout)

	a.Flag("query.max-concurrency",
		"Maximum number of queries executed concurrently.").
		Default("20").IntVar(&cfg.queryConcurrency)

	promlogflag.AddFlags(a, &cfg.logLevel)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		return 2
	}

	logger := promlog.New(cfg.logLevel)

	level.Info(logger).Log("msg", "starting prometheus mixer", "version", version.Info())
	level.Info(logger).Log("build_context", version.BuildContext())

	cfg.mixer.ExternalLabels = model.LabelSet(cfg.externalLabels)
	m, err := newMixer(log.With(logger, "component", "mixer"), cfg.mixer)
	if err != nil {
		level.Error(logger).Log("msg", "failed to initialize mixer", "err", err)
	}

	promql.LookbackDelta = time.Duration(cfg.lookbackDelta)
	queryEngine := promql.NewEngine(
		log.With(logger, "component", "query engine"),
		registerer,
		cfg.queryConcurrency,
		cfg.queryTimeout,
	)

	err = m.run(queryEngine)
	if err != nil {
		level.Error(logger).Log("msg", "error running mixer", "err", err)
		return 2
	}
	return 0
}

func main() {
	os.Exit(run())
}
