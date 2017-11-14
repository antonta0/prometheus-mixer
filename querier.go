package main

import (
	"container/heap"
	"context"
	"math"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
)

const (
	defaultScrapeInterval = int64((60 * time.Second) / time.Millisecond)
	scrapeIntervalStep    = int64((5 * time.Second) / time.Millisecond)
)

// querier queries from multiple remote read clients.
type querier struct {
	ctx        context.Context
	mint, maxt int64
	client     *multiClient
}

// Select returns a set of series that match the given label matchers.
func (q *querier) Select(p *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	query, err := remote.ToQuery(q.mint, q.maxt, matchers, p)
	if err != nil {
		return errSeriesSet{err: err}, err
	}

	res, err := q.client.read(q.ctx, query)
	if err != nil {
		return errSeriesSet{err: err}, err
	}
	sets := make([]storage.SeriesSet, 0, len(res))
	for _, r := range res {
		sets = append(sets, fromProtoQueryResult(r))
	}
	return newSeriesSet(sets, scrapeIntervalStep), nil
}

// LabelValues return all potential values for a label name.
func (q *querier) LabelValues(name string) ([]string, error) {
	return nil, nil
}

// Close releases the resources of the Querier.
func (q *querier) Close() error {
	return nil
}

// errSeriesSet implements storage.SeriesSet which always returns error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool         { return false }
func (errSeriesSet) At() storage.Series { return nil }
func (e errSeriesSet) Err() error       { return e.err }

// seriesSet implements storage.SeriesSet for a list of SeriesSet.
type seriesSet struct {
	step       int64
	currLabels labels.Labels
	currSets   []storage.SeriesSet
	sets       []storage.SeriesSet
	heap       seriesSetHeap
}

func newSeriesSet(sets []storage.SeriesSet, step int64) *seriesSet {
	var h seriesSetHeap
	for _, set := range sets {
		if set.Next() {
			heap.Push(&h, set)
		}
	}
	return &seriesSet{
		step: step,
		sets: sets,
		heap: h,
	}
}

func (s *seriesSet) Next() bool {
	for _, set := range s.currSets {
		if set.Next() {
			heap.Push(&s.heap, set)
		}
	}
	if len(s.heap) == 0 {
		return false
	}

	s.currSets = nil
	s.currLabels = s.heap[0].At().Labels()
	for len(s.heap) > 0 && labels.Equal(s.currLabels, s.heap[0].At().Labels()) {
		set := heap.Pop(&s.heap).(storage.SeriesSet)
		s.currSets = append(s.currSets, set)
	}
	return true
}

func (s *seriesSet) At() storage.Series {
	ss := make([]storage.Series, 0, len(s.currSets))
	for _, set := range s.currSets {
		ss = append(ss, set.At())
	}
	return newSeries(s.currLabels, ss, s.step)
}

func (s *seriesSet) Err() error {
	return nil
}

type seriesSetHeap []storage.SeriesSet

func (h seriesSetHeap) Len() int      { return len(h) }
func (h seriesSetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesSetHeap) Less(i, j int) bool {
	a, b := h[i].At().Labels(), h[j].At().Labels()
	return labels.Compare(a, b) < 0
}

func (h *seriesSetHeap) Push(x interface{}) {
	*h = append(*h, x.(storage.SeriesSet))
}

func (h *seriesSetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// series implements storage.Series for a list of Series with same labels.
type series struct {
	labels labels.Labels
	series []storage.Series
	dt     int64
}

// newSeries creates a new series.
func newSeries(ls labels.Labels, ss []storage.Series, step int64) storage.Series {
	if step <= 0 {
		panic("step must be greater than 0")
	}

	var dt int64 = math.MaxInt64
	for _, s := range ss {
		if i := getInterval(s.Iterator()); i < dt {
			dt = i
		}
	}
	if dt == math.MaxInt64 {
		dt = defaultScrapeInterval
	}
	if dt = round(dt, step); dt == 0 {
		dt = step
	}
	return &series{
		labels: ls,
		series: ss,
		dt:     dt,
	}
}

// Labels returns the complete set of labels identifying the series.
func (s *series) Labels() labels.Labels {
	return s.labels
}

// Iterator returns a new iterator of the data of the series.
func (s *series) Iterator() storage.SeriesIterator {
	its := make([]storage.SeriesIterator, 0, len(s.series))
	for _, s := range s.series {
		its = append(its, s.Iterator())
	}
	return newSeriesIterator(its, s.dt)
}

// getInterval picks a shortest time interval between iterator samples.
// Returns maximum int64 if iterator contains less than 2 samples.
func getInterval(it storage.SeriesIterator) int64 {
	var dt int64 = math.MaxInt64
	if !it.Next() {
		return dt
	}
	var prev, curr int64
	prev, _ = it.At()
	for i := 0; it.Next() && i < 2; i++ {
		curr, _ = it.At()
		if v := curr - prev; v > 0 && v < dt {
			dt = v
		}
		prev = curr
	}
	return dt
}

// round rounds v to the nearest multiple of m.
func round(v int64, m int64) int64 {
	r := v % m
	if r > m/2 {
		return v - r + m
	}
	return v - r
}

// seriesIterator merges and deduplicates samples from multiple iterators.
type seriesIterator struct {
	currTs  int64
	currVal float64
	its     []storage.SeriesIterator
	ts, dt  int64
}

// newSeriesIterator created a new seriesIterator.
func newSeriesIterator(iterators []storage.SeriesIterator, dt int64) storage.SeriesIterator {
	// Convert iterators to safe iterators
	its := make([]storage.SeriesIterator, 0, len(iterators))
	for _, it := range iterators {
		its = append(its, &safeIterator{it: it})
	}

	// Initialize iterators and find the smallest timestamp
	var ts int64 = math.MaxInt64
	for _, it := range its {
		if it.Next() {
			if t, _ := it.At(); t < ts {
				ts = t
			}
		}
	}
	ts = ts - (ts % dt)
	return &seriesIterator{its: its, currTs: -1, ts: ts, dt: dt}
}

// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (i *seriesIterator) Seek(t int64) bool {
	i.ts = t - (t % i.dt)
	for _, it := range i.its {
		it.Seek(t)
	}
	return i.Next()
}

// At returns the current timestamp/value pair.
func (i *seriesIterator) At() (int64, float64) {
	if i.currTs == -1 {
		panic(".At() called after .Next() returned false")
	}
	return i.currTs, i.currVal
}

// Next advances the iterator by one.
func (i *seriesIterator) Next() bool {
	for i.currTs = -1; i.currTs == -1; i.ts += i.dt {
		drained := true
		for _, it := range i.its {
			// Advance iterator as long as it has samples
			for t, v := it.At(); t >= 0; t, v = it.At() {
				drained = false
				// Skip iterator if timestamp is outside current interval
				if t >= i.ts+i.dt {
					break
				}
				// Pick the sample with the latest timestamp
				if t >= i.currTs {
					i.currTs, i.currVal = t, v
				}
				it.Next()
			}
		}
		if drained {
			break
		}
	}
	return i.currTs != -1
}

// Err returns the current error.
func (i *seriesIterator) Err() error {
	for _, it := range i.its {
		if err := it.Err(); err != nil {
			return err
		}
	}
	return nil
}

// safeIterator is a safe version of storage.SeriesIterator.
type safeIterator struct {
	next bool
	it   storage.SeriesIterator
}

// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (i *safeIterator) Seek(t int64) bool {
	i.next = i.it.Seek(t)
	return i.next
}

// At returns the current timestamp/value pair.
func (i *safeIterator) At() (int64, float64) {
	if i.next {
		return i.it.At()
	}
	return -1, -1
}

// Next advances the iterator by one.
func (i *safeIterator) Next() bool {
	i.next = i.it.Next()
	return i.next
}

// Err returns the current error.
func (i *safeIterator) Err() error {
	return i.it.Err()
}
