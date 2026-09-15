package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// Domain types
type insertRequest struct {
	MessageID  string `json:"message_id"`
	ClientName string `json:"client_name"`
	Msg        string `json:"msg"`
}

type insertResponse struct {
	OK        bool   `json:"ok"`
	Duplicate bool   `json:"duplicate"`
	MessageID string `json:"message_id"`
}

type feedItem struct {
	ID          interface{} `json:"id"`
	MessageID   string      `json:"message_id"`
	ClientName1 string      `json:"client-name"`
	ClientName2 string      `json:"client_name"`
	Msg1        string      `json:"msg"`
	Msg2        string      `json:"message"`
	Timestamp   string      `json:"timestamp"`
	CreatedAt   string      `json:"created_at"`
}

// In-Memory State for Zero-Latency Operations
var (
	stateMu sync.RWMutex
	seenIDs = make(map[string]bool)

	feedMu     sync.Mutex
	feedBuf    bytes.Buffer
	atomicFeed atomic.Pointer[[]byte]

	insertChan = make(chan insertRequest, 100000)
)

func initFeed() {
	feedMu.Lock()
	defer feedMu.Unlock()
	feedBuf.Reset()
	feedBuf.WriteString("[]")
	empty := []byte("[]")
	atomicFeed.Store(&empty)
}

func updateFeedSnapshot() {
	snap := make([]byte, feedBuf.Len())
	copy(snap, feedBuf.Bytes())
	atomicFeed.Store(&snap)
}

func loadInitialData(db *sql.DB) error {
	rows, err := db.Query("SELECT message_id, client_name, msg, created_at FROM messages ORDER BY id ASC")
	if err != nil {
		return err
	}
	defer rows.Close()

	stateMu.Lock()
	feedMu.Lock()
	defer stateMu.Unlock()
	defer feedMu.Unlock()

	feedBuf.Reset()
	feedBuf.WriteString("[]")
	seenIDs = make(map[string]bool)

	count := 0
	for rows.Next() {
		var mid, cname, msg, ts string
		if err := rows.Scan(&mid, &cname, &msg, &ts); err != nil {
			continue
		}
		seenIDs[mid] = true
		item := feedItem{
			ID:          mid,
			MessageID:   mid,
			ClientName1: cname,
			ClientName2: cname,
			Msg1:        msg,
			Msg2:        msg,
			Timestamp:   ts,
			CreatedAt:   ts,
		}
		itemJSON, _ := json.Marshal(item)
		if feedBuf.Len() <= 2 {
			feedBuf.Reset()
			feedBuf.WriteByte('[')
			feedBuf.Write(itemJSON)
			feedBuf.WriteByte(']')
		} else {
			feedBuf.Truncate(feedBuf.Len() - 1)
			feedBuf.WriteByte(',')
			feedBuf.Write(itemJSON)
			feedBuf.WriteByte(']')
		}
		count++
	}

	updateFeedSnapshot()
	log.Printf("Loaded %d existing messages into memory", count)
	return nil
}

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA cache_size = 5000;",
		"PRAGMA busy_timeout = 10000;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			log.Printf("pragma error: %v", err)
		}
	}

	const ddl = `CREATE TABLE IF NOT EXISTS messages (
		id          INTEGER  PRIMARY KEY AUTOINCREMENT,
		message_id  TEXT     UNIQUE NOT NULL,
		client_name TEXT     NOT NULL DEFAULT '',
		msg         TEXT     NOT NULL DEFAULT '',
		created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	if _, err := db.Exec(ddl); err != nil {
		return nil, err
	}

	return db, nil
}

func backgroundWriter(db *sql.DB) {
	stmt, err := db.Prepare("INSERT OR IGNORE INTO messages (message_id, client_name, msg) VALUES (?, ?, ?)")
	if err != nil {
		log.Fatalf("prepare statement error: %v", err)
	}
	defer stmt.Close()

	batch := make([]insertRequest, 0, 200)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		tx, err := db.Begin()
		if err != nil {
			log.Printf("tx begin error: %v", err)
			batch = batch[:0]
			return
		}
		txStmt := tx.Stmt(stmt)
		for _, req := range batch {
			txStmt.Exec(req.MessageID, req.ClientName, req.Msg)
		}
		txStmt.Close()
		if err := tx.Commit(); err != nil {
			log.Printf("tx commit error: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case req := <-insertChan:
			batch = append(batch, req)
			if len(batch) >= 200 {
				flush()
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flush()
			}
		}
	}
}

func handleInsert() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req insertRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		if req.MessageID == "" || req.Msg == "" {
			http.Error(w, "missing required fields", http.StatusBadRequest)
			return
		}

		stateMu.Lock()
		if seenIDs[req.MessageID] {
			stateMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(insertResponse{
				OK:        true,
				Duplicate: true,
				MessageID: req.MessageID,
			})
			return
		}
		seenIDs[req.MessageID] = true
		stateMu.Unlock()

		nowStr := time.Now().Format("2006-01-02 15:04:05")
		newItem := feedItem{
			ID:          req.MessageID,
			MessageID:   req.MessageID,
			ClientName1: req.ClientName,
			ClientName2: req.ClientName,
			Msg1:        req.Msg,
			Msg2:        req.Msg,
			Timestamp:   nowStr,
			CreatedAt:   nowStr,
		}

		itemJSON, _ := json.Marshal(newItem)

		feedMu.Lock()
		if feedBuf.Len() <= 2 {
			feedBuf.Reset()
			feedBuf.WriteByte('[')
			feedBuf.Write(itemJSON)
			feedBuf.WriteByte(']')
		} else {
			feedBuf.Truncate(feedBuf.Len() - 1)
			feedBuf.WriteByte(',')
			feedBuf.Write(itemJSON)
			feedBuf.WriteByte(']')
		}
		updateFeedSnapshot()
		feedMu.Unlock()

		// Queue for persistent SQLite batch commit
		select {
		case insertChan <- req:
		default:
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(insertResponse{
			OK:        true,
			Duplicate: false,
			MessageID: req.MessageID,
		})
	}
}

func handleFeed() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		feedPtr := atomicFeed.Load()
		if feedPtr == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", "2")
			w.Write([]byte("[]"))
			return
		}

		data := *feedPtr
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Write(data)
	}
}

func handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	}
}

func handleReset(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stateMu.Lock()
		seenIDs = make(map[string]bool)
		stateMu.Unlock()

		feedMu.Lock()
		feedBuf.Reset()
		feedBuf.WriteString("[]")
		empty := []byte("[]")
		atomicFeed.Store(&empty)
		feedMu.Unlock()

		// Drain insert queue
		for {
			select {
			case <-insertChan:
			default:
				goto Drained
			}
		}
	Drained:
		db.Exec("DELETE FROM messages; VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","cleared":true}`))
	}
}

func main() {
	addr := flag.String("addr", ":5000", "Listen address")
	dbPath := flag.String("db", "/home/student/chat_app/messages.db", "SQLite DB path")
	flag.Parse()

	db, err := openDB(*dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	initFeed()

	if err := loadInitialData(db); err != nil {
		log.Fatalf("load initial data: %v", err)
	}

	go backgroundWriter(db)

	mux := http.NewServeMux()
	mux.HandleFunc("/db/insert", handleInsert())
	mux.HandleFunc("/db/feed", handleFeed())
	mux.HandleFunc("/db/reset", handleReset(db))
	mux.HandleFunc("/reset", handleReset(db))
	mux.HandleFunc("/health", handleHealth())

	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Printf("db6_service listening on %s (db=%s)", *addr, *dbPath)
	log.Fatal(srv.ListenAndServe())
}
