// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Inspired / copied / modified from https://gitlab.com/cznic/strutil/blob/master/strutil.go,
// which is MIT licensed, so:
//
// Copyright (c) 2014 The strutil Authors. All rights reserved.

package intern

import (
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/atomic"
)

// Shared interner
var (
	Global Interner = New(prometheus.DefaultRegisterer)
)

// Iterner is a string interner.
type Interner interface {
	// Metrics returns Metrics for the interner.
	Metrics() *Metrics

	// Intern will intern an input string, returning the interned string as
	// a result.
	Intern(string) string

	// Release removes an interned string from interner.
	Release(string)
}

type Metrics struct {
	Strings             prometheus.Gauge
	NoReferenceReleases prometheus.Counter
}

func NewMetrics(r prometheus.Registerer) *Metrics {
	var m Metrics
	m.Strings = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "prometheus",
		Subsystem: "interner",
		Name:      "num_strings",
		Help:      "The current number of interned strings",
	})
	m.NoReferenceReleases = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "prometheus",
		Subsystem: "interner",
		Name:      "string_interner_zero_reference_releases_total",
		Help:      "The number of times release has been called for strings that are not interned.",
	})

	if r != nil {
		r.MustRegister(m.Strings)
		r.MustRegister(m.NoReferenceReleases)
	}

	return &m
}

func New(r prometheus.Registerer) Interner {
	stripeSize := 1 << 16

	p := &pool{
		m:     NewMetrics(r),
		size:  stripeSize,
		locks: make([]stripeLock, stripeSize),
		pools: make([]map[string]*entry, stripeSize),
	}

	for i := range p.pools {
		p.pools[i] = map[string]*entry{}
	}

	return p
}

type pool struct {
	m *Metrics

	size  int
	locks []stripeLock
	pools []map[string]*entry
}

type stripeLock struct {
	sync.Mutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

type entry struct {
	refs atomic.Int64
	s    string
}

func newEntry(s string) *entry {
	return &entry{s: s}
}

func (p *pool) Metrics() *Metrics { return p.m }

func (p *pool) Intern(s string) string {
	if s == "" {
		return ""
	}

	h := hash(s)
	stripe := p.lockFor(h)
	defer p.unlockFor(h)

	interned, ok := p.pools[stripe][s]
	if ok {
		interned.refs.Inc()
		return interned.s
	}

	p.m.Strings.Inc()
	p.pools[stripe][s] = newEntry(s)
	p.pools[stripe][s].refs.Store(1)
	return s
}

func (p *pool) Release(s string) {
	h := hash(s)
	stripe := p.lockFor(h)
	defer p.unlockFor(h)

	interned, ok := p.pools[stripe][s]
	if !ok {
		p.m.NoReferenceReleases.Inc()
		return
	}

	refs := interned.refs.Dec()
	if refs > 0 {
		return
	}

	p.m.Strings.Dec()
	delete(p.pools[stripe], s)
}

func (p *pool) lockFor(h uint64) uint64 {
	stripe := h & uint64(p.size-1)
	p.locks[stripe].Lock()
	return stripe
}

func (p *pool) unlockFor(h uint64) {
	stripe := h & uint64(p.size-1)
	p.locks[stripe].Unlock()
}

// InternLabels is a helper function for interning all label
// names and values to a given interner.
func InternLabels(interner Interner, lbls labels.Labels) {
	for i, l := range lbls {
		lbls[i].Name = interner.Intern(l.Name)
		lbls[i].Value = interner.Intern(l.Value)
	}
}

// ReleaseLabels is a helper function for releasing all label
// names and values from a given interner.
func ReleaseLabels(interner Interner, ls labels.Labels) {
	for _, l := range ls {
		interner.Release(l.Name)
		interner.Release(l.Value)
	}
}

func hash(s string) uint64 {
	return xxhash.Sum64([]byte(s))
}
