package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	stdlog "log"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/net/netutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	conntrack "github.com/mwitkow/go-conntrack"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	apiv1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/prometheus/tsdb"
)

// mixer is a main program object.
type mixer struct {
	apiHandler http.Handler
	client     *multiClient

	options mixerOptions
	logger  log.Logger
}

// mixerOptions is a set of options for the mixer.
type mixerOptions struct {
	Backends           []*url.URL
	BackendDialTimeout time.Duration
	BackendTimeout     time.Duration
	ExternalLabels     model.LabelSet
	ListenAddress      string
	MaxConnections     int
	ReadTimeout        time.Duration
}

// newMixer initializes new mixer.
func newMixer(logger log.Logger, o mixerOptions) (*mixer, error) {
	clients := []*client{}
	for _, u := range o.Backends {
		clients = append(clients, newClient(u, o.ExternalLabels, o.BackendTimeout, o.BackendDialTimeout))
	}
	return &mixer{
		client:  &multiClient{clients: clients, logger: logger},
		options: o,
		logger:  logger,
	}, nil
}

// StartTime returns the oldest timestamp stored in the storage.
func (m *mixer) StartTime() (int64, error) {
	return int64(model.Latest), nil
}

// Querier returns a new Querier on the storage.
func (m *mixer) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &querier{
		ctx:    ctx,
		mint:   mint,
		maxt:   maxt,
		client: m.client,
	}, nil
}

// Appender returns a new appender against the storage.
func (m *mixer) Appender() (storage.Appender, error) {
	return nil, errors.New("not implemented")
}

// Close closes the storage and all its underlying resources.
func (m *mixer) Close() error {
	return nil
}

func (m *mixer) getConfig() config.Config {
	cfg := config.DefaultConfig
	cfg.GlobalConfig.ExternalLabels = m.options.ExternalLabels
	return cfg
}

// run starts an http server and waits for a shutdown event.
func (m *mixer) run(q *promql.Engine) error {
	readyFn := func(h http.HandlerFunc) http.HandlerFunc { return h }
	dbFn := func() *tsdb.DB { return nil }
	api := apiv1.NewAPI(q, m, &noopRetriever{}, &noopRetriever{}, m.getConfig, nil, readyFn, dbFn, false)
	router := route.New()
	api.Register(router)
	m.apiHandler = http.StripPrefix("/api/v1", router)

	mux := http.NewServeMux()
	mux.Handle("/", m)
	mux.HandleFunc("/api/v1/query", m.handleAPI)
	mux.HandleFunc("/api/v1/query_range", m.handleAPI)
	mux.HandleFunc("/api/v1/read", m.handleAPI)

	metrics := promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
	mux.HandleFunc("/metrics", prometheus.InstrumentHandler("prometheus", metrics))

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "prometheus-mixer is healthy\n")
	})
	mux.HandleFunc("/-/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "prometheus-mixer is ready\n")
	})

	listener, err := net.Listen("tcp", m.options.ListenAddress)
	if err != nil {
		return err
	}
	listener = netutil.LimitListener(listener, m.options.MaxConnections)
	listener = conntrack.NewListener(listener,
		conntrack.TrackWithName("http"),
		conntrack.TrackWithTracing())

	errlog := stdlog.New(log.NewStdlibAdapter(level.Error(m.logger)), "", 0)

	httpsrv := &http.Server{
		Handler:     mux,
		ErrorLog:    errlog,
		ReadTimeout: m.options.ReadTimeout,
	}

	errch := make(chan error, 1)
	go func() {
		if err := httpsrv.Serve(listener); err != nil {
			errch <- err
		}
	}()
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errch:
		return err
	case sig := <-sigch:
		level.Warn(m.logger).Log("msg", "received signal", "signal", sig)
		break
	}
	level.Info(m.logger).Log("msg", "shutting down gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := httpsrv.Shutdown(ctx); err != nil {
		level.Warn(m.logger).Log("msg", "error shutting down", "err", err)
	}
	cancel()
	return nil
}

// ServeHTTP serves mixer index page.
func (m *mixer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.New("index").Parse(indexTemplate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := struct {
		ListenAddress                   string
		Version, Branch, Revision       string
		GoVersion, BuildUser, BuildDate string
		Backends                        []*url.URL
	}{
		m.options.ListenAddress,
		version.Version, version.Branch, version.Revision,
		version.GoVersion, version.BuildUser, version.BuildDate,
		m.options.Backends,
	}
	w.Header().Add("Content-type", "text/html")
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleAPI handles Prometheus API requests.
func (m *mixer) handleAPI(w http.ResponseWriter, r *http.Request) {
	var fb failureBehavior
	switch r.Header.Get("X-Prometheus-Mixer-Failure-Behavior") {
	case "any":
		fb = failureBehaviorAny
	case "", "all":
		fb = failureBehaviorAll
	default:
		http.Error(w, "unsupported value for X-Prometheus-Mixer-Failure-Behavior header\n", http.StatusBadRequest)
		return
	}
	ctx := context.WithValue(r.Context(), failureBehaviorContextKey, fb)
	m.apiHandler.ServeHTTP(w, r.WithContext(ctx))
}

// noopRetriever implements alertmanager and target retriever interfaces.
type noopRetriever struct{}

// TargetsActive Implements api.targetRetriever.
func (n *noopRetriever) TargetsActive() []*scrape.Target { return nil }

// TargetsDropped Implements api.targetRetriever.
func (n *noopRetriever) TargetsDropped() []*scrape.Target { return nil }

// Alertmanagers implements api.alertmanagerRetriever.
func (n *noopRetriever) Alertmanagers() []*url.URL { return nil }

// DroppedAlertmanagers implements api.alertmanagerRetriever.
func (n *noopRetriever) DroppedAlertmanagers() []*url.URL { return nil }

const indexTemplate = `
<html>
<head>
<title>prometheus-mixer on {{.ListenAddress}}</title>
</head>
<body>
<h1>prometheus-mixer on {{.ListenAddress}}</h1>
<p>VersionInfo: version={{.Version}} branch={{.Branch}} revision={{.Revision}}</p>
<p>BuildInfo: go={{.GoVersion}} user={{.BuildUser}} date={{.BuildDate}}</p>
<p><a href="/metrics">metrics</a> <a href="/debug/pprof">debug/pprof</a></p>
<h2>Backends</h2>
{{range .Backends}}
<p><a href="{{.}}">{{.}}</a></p>
{{end}}
</body>
</html>
`
