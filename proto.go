package main

import (
	"fmt"
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

// fromProtoQueryResult returns a storage.SeriesSet over protobuf query result.
func fromProtoQueryResult(res *prompb.QueryResult) storage.SeriesSet {
	series := make([]storage.Series, 0, len(res.Timeseries))
	for _, ts := range res.Timeseries {
		s, err := newProtoSeries(ts.Labels, ts.Samples)
		if err != nil {
			return errSeriesSet{err: err}
		}
		series = append(series, s)
	}
	return newSliceSeriesSet(series)
}

// sliceSeriesSet implements storage.SeriesSet over a slice of series.
type sliceSeriesSet struct {
	cur    int
	series []storage.Series
}

func newSliceSeriesSet(s []storage.Series) storage.SeriesSet {
	return &sliceSeriesSet{cur: -1, series: s}
}

func (s *sliceSeriesSet) Next() bool {
	s.cur++
	return s.cur < len(s.series)
}

func (s *sliceSeriesSet) At() storage.Series {
	return s.series[s.cur]
}

func (s *sliceSeriesSet) Err() error {
	return nil
}

// protoSeries implementes storage.Series over protobuf samples.
type protoSeries struct {
	labels  labels.Labels
	samples []*prompb.Sample
}

func newProtoSeries(labelPairs []*prompb.Label, samples []*prompb.Sample) (storage.Series, error) {
	ls, err := labelsProtoToLabels(labelPairs)
	if err != nil {
		return nil, err
	}
	return &protoSeries{labels: ls, samples: samples}, nil
}

func (p *protoSeries) Labels() labels.Labels {
	return labels.New(p.labels...)
}

func (p *protoSeries) Iterator() storage.SeriesIterator {
	return &protoSeriesIterator{cur: -1, samples: p.samples}
}

// labelsProtoToLabels validates and converts proto labelPairs to Labels.
func labelsProtoToLabels(labelPairs []*prompb.Label) (labels.Labels, error) {
	ls := make(labels.Labels, 0, len(labelPairs))
	for _, lp := range labelPairs {
		l := labels.Label{Name: lp.Name, Value: lp.Value}
		if l.Name == labels.MetricName && !model.IsValidMetricName(model.LabelValue(l.Value)) {
			return nil, fmt.Errorf("invalid metric name: %v", l.Value)
		}
		if !model.LabelName(l.Name).IsValid() {
			return nil, fmt.Errorf("invalid label name: %v", l.Name)
		}
		if !model.LabelValue(l.Value).IsValid() {
			return nil, fmt.Errorf("invalid label value: %v", l.Value)
		}
		ls = append(ls, l)
	}
	return ls, nil
}

// protoSeriesIterator implements storage.SeriesIterator over protobuf samples.
type protoSeriesIterator struct {
	cur     int
	samples []*prompb.Sample
}

func (p *protoSeriesIterator) Seek(t int64) bool {
	p.cur = sort.Search(len(p.samples), func(n int) bool {
		return p.samples[n].Timestamp >= t
	})
	return p.cur < len(p.samples)
}

func (p *protoSeriesIterator) At() (t int64, v float64) {
	s := p.samples[p.cur]
	return s.Timestamp, s.Value
}

func (p *protoSeriesIterator) Next() bool {
	p.cur++
	return p.cur < len(p.samples)
}

func (p *protoSeriesIterator) Err() error {
	return nil
}
