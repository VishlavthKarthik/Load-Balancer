package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"flag"
	"io"
	"io/fs"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed static/*
var staticFS embed.FS

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

type Backend struct {
	ID        string
	URL       *url.URL
	Transport *http.Transport

	activeReqs int64 // atomic
	totalReqs  int64 // atomic
	totalErrs  int64 // atomic

	mu    sync.RWMutex
	emaMS float64
	alpha float64
}

func newBackend(id, rawURL string, transport *http.Transport) (*Backend, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	return &Backend{
		ID:        id,
		URL:       u,
		Transport: transport,
		alpha:     0.15,
		emaMS:     10.0,
	}, nil
}

func (b *Backend) Score() float64 {
	active := float64(atomic.LoadInt64(&b.activeReqs))
	if active < 0 {
		active = 0
	}
	b.mu.RLock()
	ema := b.emaMS
	if math.IsNaN(ema) || math.IsInf(ema, 0) {
		ema = 10.0
	}
	b.mu.RUnlock()

	// Backend 1 on Sys2 shares CPU with LB and DB, so weight it slightly higher (1.25x)
	// so Sys3 and Sys4 with dedicated CPUs take slightly more load
	weight := 1.0
	if b.ID == "backend-1" {
		weight = 1.25
	}
	return (active*3.0 + ema*0.1) * weight
}

func (b *Backend) RecordSuccess(ms float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.emaMS = b.alpha*ms + (1-b.alpha)*b.emaMS
}

func (b *Backend) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.emaMS = b.emaMS * 1.2
	if b.emaMS > 200.0 {
		b.emaMS = 200.0
	}
}

// ---------------------------------------------------------------------------
// Load Balancer Pool
// ---------------------------------------------------------------------------

type Pool struct {
	backends    []*Backend
	totalRouted int64
}

func (p *Pool) SelectBackend(exclude map[string]bool) *Backend {
	var best *Backend
	bestScore := math.MaxFloat64

	for _, b := range p.backends {
		if exclude != nil && exclude[b.ID] {
			continue
		}
		score := b.Score()
		if score < bestScore {
			bestScore = score
			best = b
		}
	}

	if best != nil {
		return best
	}
	return p.backends[0]
}

// ---------------------------------------------------------------------------
// Proxy and Route Handlers
// ---------------------------------------------------------------------------

var (
	pool      *Pool
	dbClient  *http.Client
	dbFeedURL = "http://127.0.0.1:5000/db/feed"
)

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&pool.totalRouted, 1)

	var bodyBytes []byte
	if r.Body != nil {
		bodyBytes, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}

	excluded := make(map[string]bool)
	for attempt := 0; attempt < len(pool.backends); attempt++ {
		b := pool.SelectBackend(excluded)
		if b == nil {
			break
		}

		atomic.AddInt64(&b.activeReqs, 1)
		atomic.AddInt64(&b.totalReqs, 1)
		t0 := time.Now()

		outReq := r.Clone(r.Context())
		outReq.URL.Scheme = b.URL.Scheme
		outReq.URL.Host = b.URL.Host
		outReq.Host = b.URL.Host
		outReq.RequestURI = ""
		if len(bodyBytes) > 0 {
			outReq.ContentLength = int64(len(bodyBytes))
			outReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		} else {
			outReq.ContentLength = 0
			outReq.Body = http.NoBody
		}

		resp, err := b.Transport.RoundTrip(outReq)
		atomic.AddInt64(&b.activeReqs, -1)
		elapsed := float64(time.Since(t0).Milliseconds())

		if err != nil || (resp != nil && resp.StatusCode >= 500) {
			atomic.AddInt64(&b.totalErrs, 1)
			b.RecordFailure()
			excluded[b.ID] = true
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}

		b.RecordSuccess(elapsed)

		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		resp.Body.Close()
		return
	}

	// Ultimate fallback to backend-1
	b1 := pool.backends[0]
	outReq := r.Clone(r.Context())
	outReq.URL.Scheme = b1.URL.Scheme
	outReq.URL.Host = b1.URL.Host
	outReq.Host = b1.URL.Host
	outReq.RequestURI = ""
	if len(bodyBytes) > 0 {
		outReq.ContentLength = int64(len(bodyBytes))
		outReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	} else {
		outReq.ContentLength = 0
		outReq.Body = http.NoBody
	}
	resp, err := b1.Transport.RoundTrip(outReq)
	if err == nil && resp != nil {
		defer resp.Body.Close()
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	http.Error(w, `{"status":"error","error":"backends busy"}`, http.StatusServiceUnavailable)
}

func feedHandler(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&pool.totalRouted, 1)

	resp, err := dbClient.Get(dbFeedURL)
	if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			w.Header().Set("Content-Length", cl)
		}
		w.WriteHeader(http.StatusOK)
		io.Copy(w, resp.Body)
		return
	}
	if resp != nil {
		resp.Body.Close()
	}

	proxyHandler(w, r)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

func lbMetricsHandler(w http.ResponseWriter, r *http.Request) {
	type bstat struct {
		ID            string  `json:"id"`
		URL           string  `json:"url"`
		Healthy       bool    `json:"healthy"`
		ActiveReqs    int64   `json:"active_requests"`
		TotalReqs     int64   `json:"total_requests"`
		TotalErrs     int64   `json:"total_errors"`
		EMAResponseMS float64 `json:"ema_response_ms"`
		LoadScore     float64 `json:"load_score"`
	}

	stats := make([]bstat, 0, len(pool.backends))
	for _, b := range pool.backends {
		b.mu.RLock()
		ema := b.emaMS
		if math.IsNaN(ema) || math.IsInf(ema, 0) {
			ema = 10.0
		}
		b.mu.RUnlock()

		score := b.Score()
		act := atomic.LoadInt64(&b.activeReqs)
		if act < 0 {
			act = 0
		}

		stats = append(stats, bstat{
			ID:            b.ID,
			URL:           b.URL.String(),
			Healthy:       true,
			ActiveReqs:    act,
			TotalReqs:     atomic.LoadInt64(&b.totalReqs),
			TotalErrs:     atomic.LoadInt64(&b.totalErrs),
			EMAResponseMS: math.Round(ema*100) / 100,
			LoadScore:     math.Round(score*100) / 100,
		})
	}

	out := map[string]interface{}{
		"algorithm":    "dynamic_performance_least_load",
		"total_routed": atomic.LoadInt64(&pool.totalRouted),
		"backends":     stats,
	}

	data, _ := json.Marshal(out)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	addr := flag.String("addr", ":3000", "Listen address")
	backendsFlag := flag.String("backends",
		"backend-1=http://127.0.0.1:4000,backend-2=http://172.17.0.120:3000,backend-3=http://172.17.0.121:3000",
		"Comma-separated list of backend ID=URL pairs")
	flag.Int64("threshold-active", 35, "Ignored")
	flag.Float64("threshold-latency", 60.0, "Ignored")
	flag.Parse()

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns:          2000,
		MaxIdleConnsPerHost:   1000,
		MaxConnsPerHost:       2000,
		IdleConnTimeout:       120 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		DisableKeepAlives:     false,
	}

	dbClient = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 60 * time.Second,
			}).DialContext,
			MaxIdleConns:          500,
			MaxIdleConnsPerHost:   250,
			MaxConnsPerHost:       500,
			IdleConnTimeout:       120 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second,
		},
		Timeout: 30 * time.Second,
	}

	pool = &Pool{}

	for _, item := range strings.Split(*backendsFlag, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		var id, rawURL string
		if len(parts) == 2 {
			id, rawURL = parts[0], parts[1]
		} else {
			id, rawURL = item, item
		}

		b, err := newBackend(id, rawURL, transport)
		if err != nil {
			log.Fatalf("invalid backend URL %s: %v", rawURL, err)
		}
		pool.backends = append(pool.backends, b)
		log.Printf("Registered backend: [%s] -> %s", id, rawURL)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/lb-metrics", lbMetricsHandler)
	mux.HandleFunc("/metrics", lbMetricsHandler)
	mux.HandleFunc("/message", proxyHandler)
	mux.HandleFunc("/feed", feedHandler)
	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		req, err := http.NewRequest(r.Method, "http://127.0.0.1:5000/db/reset", r.Body)
		if err != nil {
			http.Error(w, `{"status":"error"}`, http.StatusInternalServerError)
			return
		}
		resp, err := dbClient.Do(req)
		if err == nil && resp != nil {
			defer resp.Body.Close()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return
		}
		http.Error(w, `{"status":"error"}`, http.StatusInternalServerError)
	})

	// Serve static UI from embedded static/
	subFS, err := fs.Sub(staticFS, "static")
	if err == nil {
		fileServer := http.FileServer(http.FS(subFS))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" || r.URL.Path == "/index.html" || r.URL.Path == "/style.css" || r.URL.Path == "/app.js" || r.URL.Path == "/crypto.js" {
				fileServer.ServeHTTP(w, r)
				return
			}
			proxyHandler(w, r)
		})
	} else {
		mux.HandleFunc("/", proxyHandler)
	}

	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("Dynamic Performance Load Balancer listening on %s", *addr)
	log.Fatal(srv.ListenAndServe())
}
