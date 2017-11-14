package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type sample struct {
	t int64
	v float64
}

func (s *sample) String() string {
	return fmt.Sprintf("t: %d v:%f", s.t, s.v)
}

func TestSeriesSet(t *testing.T) {
	for _, tc := range []struct {
		input    []storage.SeriesSet
		expected storage.SeriesSet
	}{
		{
			input:    []storage.SeriesSet{newTestSeriesSet()},
			expected: newTestSeriesSet(),
		},
		{
			input: []storage.SeriesSet{newTestSeriesSet(
				newTestSeries(labels.FromStrings("bar", "baz"), []sample{{1, 1}, {2, 2}}),
				newTestSeries(labels.FromStrings("foo", "bar"), []sample{{0, 0}, {1, 1}}),
			)},
			expected: newTestSeriesSet(
				newTestSeries(labels.FromStrings("bar", "baz"), []sample{{1, 1}, {2, 2}}),
				newTestSeries(labels.FromStrings("foo", "bar"), []sample{{0, 0}, {1, 1}}),
			),
		},
		{
			input: []storage.SeriesSet{newTestSeriesSet(
				newTestSeries(labels.FromStrings("foo", "bar"), []sample{{0, 0}, {1, 1}}),
			), newTestSeriesSet(
				newTestSeries(labels.FromStrings("bar", "baz"), []sample{{1, 1}, {2, 2}}),
			)},
			expected: newTestSeriesSet(
				newTestSeries(labels.FromStrings("bar", "baz"), []sample{{1, 1}, {2, 2}}),
				newTestSeries(labels.FromStrings("foo", "bar"), []sample{{0, 0}, {1, 1}}),
			),
		},
		{
			input: []storage.SeriesSet{newTestSeriesSet(
				newTestSeries(labels.FromStrings("bar", "baz"), []sample{{1, 1}, {2, 2}}),
				newTestSeries(labels.FromStrings("foo", "bar"), []sample{{0, 0}, {1, 1}}),
			), newTestSeriesSet(
				newTestSeries(labels.FromStrings("bar", "baz"), []sample{{3, 3}, {4, 4}}),
				newTestSeries(labels.FromStrings("foo", "bar"), []sample{{2, 2}, {3, 3}}),
			)},
			expected: newTestSeriesSet(
				newTestSeries(labels.FromStrings("bar", "baz"), []sample{{1, 1}, {2, 2}, {3, 3}, {4, 4}}),
				newTestSeries(labels.FromStrings("foo", "bar"), []sample{{0, 0}, {1, 1}, {2, 2}, {3, 3}}),
			),
		},
	} {
		ss := newSeriesSet(tc.input, 1)
		for ss.Next() {
			require.True(t, tc.expected.Next())
			actual := ss.At()
			expected := tc.expected.At()
			require.Equal(t, expected.Labels(), actual.Labels())
			require.Equal(t, drainIterator(expected.Iterator()), drainIterator(actual.Iterator()))
		}
		require.False(t, tc.expected.Next())
	}
}

func TestGetInterval(t *testing.T) {
	for _, tc := range []struct {
		input    storage.SeriesIterator
		expected int64
	}{
		{
			input:    newTestSeriesIterator([]sample{}),
			expected: math.MaxInt64,
		},
		{
			input:    newTestSeriesIterator([]sample{{0, 0}}),
			expected: math.MaxInt64,
		},
		{
			input:    newTestSeriesIterator([]sample{{0, 0}, {1, 1}}),
			expected: 1,
		},
		{
			input:    newTestSeriesIterator([]sample{{1, 1}, {1, 1}}),
			expected: math.MaxInt64,
		},
		{
			input:    newTestSeriesIterator([]sample{{0, 0}, {2, 2}, {3, 3}}),
			expected: 1,
		},
		{
			input:    newTestSeriesIterator([]sample{{0, 0}, {2, 2}, {4, 4}, {5, 5}}),
			expected: 2,
		},
	} {
		actual := getInterval(tc.input)
		require.Equal(t, tc.expected, actual)
	}
}

func TestRound(t *testing.T) {
	for _, tc := range []struct {
		input    int64
		expected int64
	}{
		{input: 0, expected: 0},
		{input: 2, expected: 0},
		{input: 4, expected: 5},
		{input: 5, expected: 5},
		{input: 8, expected: 10},
	} {
		actual := round(tc.input, 5)
		require.Equal(t, tc.expected, actual)
	}
}

func TestSeries_Interval(t *testing.T) {
	for _, tc := range []struct {
		input    []storage.Series
		expected int64
	}{
		{
			input: []storage.Series{
				newTestSeries(nil, []sample{{0, 0}}),
				newTestSeries(nil, []sample{{1, 1}}),
			},
			expected: defaultScrapeInterval,
		},
		{
			input: []storage.Series{
				newTestSeries(nil, []sample{{0, 0}, {1, 1}}),
				newTestSeries(nil, []sample{{1, 1}, {2, 2}}),
			},
			expected: 5,
		},
		{
			input: []storage.Series{
				newTestSeries(nil, []sample{{12, 12}, {32, 32}}),
				newTestSeries(nil, []sample{{22, 22}, {32, 32}}),
			},
			expected: 10,
		},
	} {
		s := newSeries(nil, tc.input, 5).(*series)
		require.Equal(t, tc.expected, s.dt)
	}
}

func TestSeriesIterator(t *testing.T) {
	for _, tc := range []struct {
		input    []storage.SeriesIterator
		expected []sample
	}{
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{}),
				newTestSeriesIterator([]sample{}),
			},
			expected: []sample{},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{1, 1}, {3, 3}}),
			},
			expected: []sample{{1, 1}, {3, 3}},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{2, 2}}),
				newTestSeriesIterator([]sample{{3, 3}}),
			},
			expected: []sample{{3, 3}},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{1, 1}, {3, 3}}),
				newTestSeriesIterator([]sample{{2, 2}, {4, 4}}),
			},
			expected: []sample{{1, 1}, {3, 3}, {4, 4}},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{0, 0}, {1, 1}}),
				newTestSeriesIterator([]sample{{2, 2}, {3, 3}}),
			},
			expected: []sample{{1, 1}, {3, 3}},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{0, 0}, {2, 2}}),
				newTestSeriesIterator([]sample{{1, 1}, {3, 3}}),
			},
			expected: []sample{{1, 1}, {3, 3}},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{8, 8}}),
				newTestSeriesIterator([]sample{{1, 1}, {2, 2}}),
				newTestSeriesIterator([]sample{{3, 3}, {5, 5}}),
			},
			expected: []sample{{1, 1}, {3, 3}, {5, 5}, {8, 8}},
		},
	} {
		actual := drainIterator(newSeriesIterator(tc.input, 2))
		require.Equal(t, tc.expected, actual)
	}
}

func TestSeriesIterator_Seek(t *testing.T) {
	for _, tc := range []struct {
		input    []storage.SeriesIterator
		seek     int64
		expected []sample
	}{
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{}),
				newTestSeriesIterator([]sample{}),
			},
			seek:     2,
			expected: []sample{},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{0, 0}, {2, 2}, {4, 4}}),
			},
			seek:     2,
			expected: []sample{{2, 2}, {4, 4}},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{0, 0}, {1, 1}}),
				newTestSeriesIterator([]sample{{2, 2}, {3, 3}}),
			},
			seek:     2,
			expected: []sample{{3, 3}},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{0, 0}, {2, 2}}),
				newTestSeriesIterator([]sample{{1, 1}, {3, 3}}),
			},
			seek:     2,
			expected: []sample{{3, 3}},
		},
		{
			input: []storage.SeriesIterator{
				newTestSeriesIterator([]sample{{8, 8}}),
				newTestSeriesIterator([]sample{{1, 1}, {2, 2}}),
				newTestSeriesIterator([]sample{{3, 3}, {5, 5}}),
			},
			seek:     4,
			expected: []sample{{5, 5}, {8, 8}},
		},
	} {
		it := newSeriesIterator(tc.input, 2)
		actual := []sample{}
		if it.Seek(tc.seek) {
			t, v := it.At()
			actual = append(actual, sample{t, v})
		}
		actual = append(actual, drainIterator(it)...)
		require.Equal(t, tc.expected, actual)
	}
}

func newTestSeriesSet(series ...storage.Series) storage.SeriesSet {
	return newSliceSeriesSet(series)
}

type testSeries struct {
	labels  labels.Labels
	samples []sample
}

func newTestSeries(ls labels.Labels, samples []sample) storage.Series {
	return &testSeries{labels: ls, samples: samples}
}

func (s *testSeries) Labels() labels.Labels {
	return s.labels
}

func (s *testSeries) Iterator() storage.SeriesIterator {
	return newTestSeriesIterator(s.samples)
}

type testSeriesIterator struct {
	idx     int
	samples []sample
}

func newTestSeriesIterator(samples []sample) storage.SeriesIterator {
	return &testSeriesIterator{idx: -1, samples: samples}
}

func (i *testSeriesIterator) Seek(t int64) bool {
	i.idx = sort.Search(len(i.samples), func(n int) bool {
		return i.samples[n].t >= t
	})
	return i.idx < len(i.samples)
}

func (i *testSeriesIterator) At() (t int64, v float64) {
	s := i.samples[i.idx]
	return s.t, s.v
}

func (i *testSeriesIterator) Next() bool {
	i.idx++
	return i.idx < len(i.samples)
}

func (i *testSeriesIterator) Err() error {
	return nil
}

func drainIterator(it storage.SeriesIterator) []sample {
	samples := []sample{}
	for it.Next() {
		t, v := it.At()
		samples = append(samples, sample{t, v})
	}
	return samples
}

func randRange(min, max int64) int64 {
	return rand.Int63n(max-min) + min
}

func generateSeries(n, k int, step int64) []storage.Series {
	ss := make([]storage.Series, 0, k)
	for i := 0; i < k; i++ {
		samples := make([]sample, 0, n)
		for i := 0; i < n; i++ {
			t := randRange(int64(i)*step, int64(i+1)*step)
			samples = append(samples, sample{t, 1})
		}
		ss = append(ss, newTestSeries(nil, samples))
	}
	return ss
}

func BenchmarkSeriesLargeN(b *testing.B) {
	s := generateSeries(1<<20, 5, 5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = drainIterator(newSeries(nil, s, 5).Iterator())
	}
}

func BenchmarkSeriesLargeK(b *testing.B) {
	s := generateSeries(5, 1<<20, 5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = drainIterator(newSeries(nil, s, 5).Iterator())
	}
}
