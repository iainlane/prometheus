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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIntern(t *testing.T) {
	interner := New(nil).(*pool)
	testString := "TestIntern"
	interner.Intern(testString)

	stripe := interner.lockFor(hash(testString))
	interned, ok := interner.pools[stripe][testString]
	interner.unlockFor(hash(testString))

	require.Equal(t, true, ok)
	require.Equal(t, int64(1), interned.refs.Load(), fmt.Sprintf("expected refs to be 1 but it was %d", interned.refs.Load()))
}

func TestIntern_MultiRef(t *testing.T) {
	interner := New(nil).(*pool)
	testString := "TestIntern_MultiRef"

	stripe := interner.lockFor(hash(testString))
	interned, ok := interner.pools[stripe][testString]
	interner.unlockFor(hash(testString))

	require.Equal(t, true, ok)
	require.Equal(t, int64(1), interned.refs.Load(), fmt.Sprintf("expected refs to be 1 but it was %d", interned.refs.Load()))

	interner.Intern(testString)
	interned, ok = interner.pools[stripe][testString]

	require.Equal(t, true, ok)
	require.Equal(t, int64(2), interned.refs.Load(), fmt.Sprintf("expected refs to be 2 but it was %d", interned.refs.Load()))
}

func TestIntern_DeleteRef(t *testing.T) {
	interner := New(nil).(*pool)
	testString := "TestIntern_DeleteRef"

	stripe := interner.lockFor(hash(testString))
	interned, ok := interner.pools[stripe][testString]
	interner.unlockFor(hash(testString))

	require.Equal(t, true, ok)
	require.Equal(t, int64(1), interned.refs.Load(), fmt.Sprintf("expected refs to be 1 but it was %d", interned.refs.Load()))

	interner.Release(testString)
	_, ok = interner.pools[stripe][testString]
	require.Equal(t, false, ok)
}

func TestIntern_MultiRef_Concurrent(t *testing.T) {
	interner := New(nil).(*pool)
	testString := "TestIntern_MultiRef_Concurrent"

	interner.Intern(testString)
	stripe := interner.lockFor(hash(testString))
	interned, ok := interner.pools[stripe][testString]
	interner.unlockFor(hash(testString))
	require.Equal(t, true, ok)
	require.Equal(t, int64(1), interned.refs.Load(), fmt.Sprintf("expected refs to be 1 but it was %d", interned.refs.Load()))

	go interner.Release(testString)

	interner.Intern(testString)

	time.Sleep(time.Millisecond)

	interner.lockFor(hash(testString))
	interned, ok = interner.pools[stripe][testString]
	interner.unlockFor(hash(testString))
	require.Equal(t, true, ok)
	require.Equal(t, int64(1), interned.refs.Load(), fmt.Sprintf("expected refs to be 1 but it was %d", interned.refs.Load()))
}

func BenchmarkIntern(b *testing.B) {
	var (
		r        = rand.New(rand.NewSource(1))
		interner = New(nil).(*pool)
	)

	strings := generateStrings(r, b)

	internCh := make(chan string)
	releaseCh := make(chan string)

	go func() {
		for _, s := range strings {
			internCh <- s
		}
		close(internCh)

		for _, s := range strings {
			releaseCh <- s
		}
		close(releaseCh)
	}()

	var (
		wg         sync.WaitGroup
		numWorkers = 10
	)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			for s := range internCh {
				interner.Intern(s)
			}
			for s := range releaseCh {
				interner.Release(s)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func generateStrings(r *rand.Rand, b *testing.B) []string {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()

	strings := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		strings[i] = randStringRunes(r, 10)
	}
	return strings
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(r *rand.Rand, n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	return string(b)
}
