// Adapted from agent-repro-gopf/bench_load.go.
package main

import (
	"context"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type loadResult struct {
	Successful int
	Failed     int
	Elapsed    time.Duration
	Mean       time.Duration
	P50        time.Duration
	P99        time.Duration
}

func runLoad(ctx context.Context, address string, requests, concurrency int, keepAlive bool) loadResult {
	transport := &http.Transport{
		DisableKeepAlives:   !keepAlive,
		DisableCompression:  true,
		MaxConnsPerHost:     concurrency,
		MaxIdleConns:        concurrency,
		MaxIdleConnsPerHost: concurrency,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	var next, failed atomic.Int64
	var mu sync.Mutex
	lats := make([]time.Duration, 0, requests)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil && next.Add(1) <= int64(requests) {
				t0 := time.Now()
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/", nil)
				if err != nil {
					failed.Add(1)
					continue
				}
				response, err := client.Do(request)
				if err != nil {
					failed.Add(1)
					continue
				}
				_, readErr := io.Copy(io.Discard, response.Body)
				closeErr := response.Body.Close()
				if readErr != nil || closeErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
					failed.Add(1)
					continue
				}
				latency := time.Since(t0)
				mu.Lock()
				lats = append(lats, latency)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	result := loadResult{Successful: len(lats), Failed: int(failed.Load()), Elapsed: time.Since(start)}
	if len(lats) == 0 {
		return result
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var sum time.Duration
	for _, latency := range lats {
		sum += latency
	}
	result.Mean = sum / time.Duration(len(lats))
	result.P50 = lats[int(float64(len(lats)-1)*0.5)]
	result.P99 = lats[int(float64(len(lats)-1)*0.99)]
	return result
}
