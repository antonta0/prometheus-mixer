# prometheus-mixer

Prometheus mixer is a query and remote read API proxy that reads raw samples
from multiple Prometheus remote read endpoints, mixes them into time-series
normalized to a single sample per interval and runs a query on the resulting
data.

The main use case of the mixer is to provide a single query endpoint for
multiple Prometheus replicas, scraping same targets. It can fill missing samples
from one or more Prometheus instances, by "mixing" raw samples before executing
a query.

Mixer may also be used to read from long-term storage adapters, without
configuring remote read on Prometheus servers. This solves local storage access
problems on Prometheus servers when remote read backends are unavailable (see
[prometheus/prometheus#2573][storage-issue]).


## Building

There're no pre-build docker images or binaries. The only way to create
binary is to build from the source. Before building, ensure that [Go version 1.10
or greater is installed][go-installation].

Clone the repository and build via provided `Makefile`. The binary will be
created in the current directoy:

```
$ mkdir -p $GOPATH/src/github.com/mz-techops/prometheus-mixer
$ cd $GOPATH/src/github.com/mz-techops/prometheus-mixer
$ git clone https://github.com/mz-techops/prometheus-mixer.git .
$ make build
$ ./prometheus-mixer --version
```

To build a docker image with the binary compiled above, run `make docker`. Image
name and tag can be configured by `DOCKER_IMAGE_NAME` and `DOCKER_IMAGE_TAG`
variables.


## Usage

All configuration is controlled by command line flags.

Most important flag is `--mixer.backend`. It tells mixer about backends to
query raw samples from. It can be specified multiple times.

Here's an example of mixer with `prometheus-0:9090` and `prometheus-1:9090` backends:

```
$ prometheus-mixer \
    --mixer.backend=http://prometheus-0:9090/api/v1/read \
    --mixer.backend=http://prometheus-1:9090/api/v1/read
```

See `prometheus-mixer -h` for more details about available flags.


## Details

### API

Mixer partially implements Prometheus API. It should be deployed behind a L7
load balancer / proxy, that routes `/api/v1/query`, `/api/v1/query_range` and
`/api/v1/read` endpoints to the mixer. Other endpoints should be routed to
Prometheus servers directly.

API calls to the mixer by default succeed if at least one backend responds
successfully. With `X-Prometheus-Mixer-Failure-Behavior` request header set to
`any`, the API call will fail if at least one backend fails.

### External labels

External labels configured on Prometheus server are included in `query`,
`query_range` and `read` mixer endpoints, because Prometheus inserts them when
it is read by mixer. This behavior can be controlled by `--mixer.external-label`
flag (can be specified multiple times). For example, to exclude external label
`foo` from `query` and `query_range`, but re-insert it to `read` endpoint with
value `bar`, specify `--mixer.external-label=foo=bar` flag.

### Mixing implementation

**IMPORTANT:** *Mixer expects remote read backends to respond with sorted
time-series and samples. Behavior with unsorted time-series or samples is
undefined.*

Mixing is implemented by picking a single sample (timestamp, value pair) with
latest timestamp within each interval.

Consider following example, where sample interval is `15`, `s1` to `s6` are
samples available on remote read backends `R1` and `R2`, and a query that
selects data from `A` to `B`:

```
R1|  s1          |  s3          |              |  s6          |
R2|           s2 |           s4 |           s5 |              |
  +-----------------------------------------------------------+
  0       ^      15             30             45           ^ 60
          |                                                 |
          A                                                 B
```

Mixer will request all samples that fall into `A`-`B` range from backends,
resulting in `s2`, `s3`, `s4`, `s5`, and `s6`. Then, for each interval of length
`15` starting from `0` it will pick a sample with latest timestamp.
The final set of samples will be: `s2`, `s4`, `s5`, and `s6`.

### Debug info

Mixer includes following metrics from Prometheus: `http_*`, `net_conntrack_*`,
`prometheus_engine_*`.

Metrics specific to the mixer:

```
# HELP mixer_backend_queries_duration_seconds Duration of queries to backends.
# TYPE mixer_backend_queries_duration_seconds histogram
# HELP mixer_backend_queries_failed_total Total number of failed queries to backends.
# TYPE mixer_backend_queries_failed_total counter
# HELP mixer_backend_queries_total Total number of queries to backends.
# TYPE mixer_backend_queries_total counter
# HELP mixer_backend_circuit_breaker_tripped Whether a circuit breaker is tripped.
# TYPE mixer_backend_circuit_breaker_tripped gauge
# HELP mixer_backend_circuit_breaker_last_retry_timestamp_seconds Timestamp of the last retry of a circuit breaker.
# TYPE mixer_backend_circuit_breaker_last_retry_timestamp_seconds gauge
# HELP mixer_backend_circuit_breaker_last_trip_timestamp_seconds Timestamp of the last trip of a circuit breaker.
# TYPE mixer_backend_circuit_breaker_last_trip_timestamp_seconds gauge
# HELP mixer_build_info A metric with a constant '1' value labeled by version, revision, branch, and goversion from which mixer was built.
# TYPE mixer_build_info gauge
```

In addition to metrics, mixer exposes [debug endpoints][go-profiling] at
`/debug/pprof/`.

Mixer's index page contains version info, configured backends, and links to
metric and debug endpoints.


## Contributing

Use GitHub pull requests to contribute changes to the project.
Relevant coding guidelines are the [Effective Go][effective-go] and the
[Go Code Review Comments][go-code-review-comments].


## License

BSD 3-clause, see [LICENSE](/LICENSE).


[storage-issue]: https://github.com/prometheus/prometheus/issues/2573
[go-installation]: https://golang.org/doc/install
[go-profiling]: https://blog.golang.org/profiling-go-programs
[effective-go]: https://golang.org/doc/effective_go.html
[go-code-review-comments]: https://github.com/golang/go/wiki/CodeReviewComments
