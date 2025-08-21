package main

import (
	"database/sql"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Config for the load test
type Config struct {
	DBType           string   // "timescale" or "scylla"
	ConnectionString string   // DSN for Timescale
	ScyllaHosts      []string // List of Scylla hosts
	Keyspace         string   // Scylla keyspace
	WorkerCount      int      // Number of concurrent workers
	WriteCount       int      // Number of writes per worker
}

type SessionData struct {
	Time       time.Time
	SessionID  uuid.UUID
	CurrentCDN string
	Kbps       int
}

func main() {
	config := Config{
		DBType:           "timescale", // or "scylla"
		ConnectionString: os.Getenv("DB_CONN_STR"),
		ScyllaHosts:      strings.Split(os.Getenv("SCYLLA_HOSTS"), ","),
		Keyspace:         "content_steering",
		WorkerCount:      100,  // 100 concurrent clients
		WriteCount:       1000, // 1000 records each
	}

	var wg sync.WaitGroup
	startTime := time.Now()

	log.Printf("Starting load test for %s with %d workers...", config.DBType, config.WorkerCount)

	for i := 0; i < config.WorkerCount; i++ {
		wg.Add(1)
		go worker(i, config, &wg)
	}
	wg.Wait()

	totalWrites := config.WorkerCount * config.WriteCount
	duration := time.Since(startTime)
	writesPerSecond := float64(totalWrites) / duration.Seconds()

	log.Printf("Completed %d writes in %v", totalWrites, duration)
	log.Printf("Average ingest rate: %.2f writes/second\n", writesPerSecond)

	runReadQuery(config)
}

// worker simulates a single client writing data
func worker(id int, config Config, wg *sync.WaitGroup) {
	defer wg.Done()

	var session any
	var err error

	if config.DBType == "timescale" {
		db, err := sql.Open("pgx", config.ConnectionString)
		if err != nil {
			log.Printf("Worker %d failed to connect: %v", id, err)
			return
		}
		defer db.Close()
		session = db
	} else { // ScyllaDB
		cluster := gocql.NewCluster(config.ScyllaHosts...)
		cluster.Keyspace = config.Keyspace
		s, err := cluster.CreateSession()
		if err != nil {
			log.Printf("Worker %d failed to connect: %v", id, err)
			return
		}
		defer s.Close()
		session = s
	}

	for i := 0; i < config.WriteCount; i++ {
		data := SessionData{Time: time.Now(), SessionID: uuid.New(), CurrentCDN: "Akamai", Kbps: 5000}
		if config.DBType == "timescale" {
			db := session.(*sql.DB)
			_, err = db.Exec("INSERT INTO content_sessions (time, session_id, current_cdn, video_profile_kbps) VALUES ($1, $2, $3, $4)",
				data.Time, data.SessionID, data.CurrentCDN, data.Kbps)
		} else { // scylla
			s := session.(*gocql.Session)
			err = s.Query("INSERT INTO content_sessions (time, session_id, current_cdn, video_profile_kbps) VALUES (?, ?, ?, ?)",
				data.Time, data.SessionID, data.CurrentCDN, data.Kbps).Exec()
		}
		if err != nil {
			log.Printf("Worker %d write error: %v", id, err)
		}
	}
}

// runReadQuery runs a sample aggregation query to measure latency
func runReadQuery(config Config) {
	log.Println("Running sample read query...")
	startTime := time.Now()

	if config.DBType == "timescale" {
		db, err := sql.Open("pgx", config.ConnectionString)
		if err != nil {
			log.Fatalf("Read query failed to connect: %v", err)
		}
		defer db.Close()

		rows, err := db.Query(`
			SELECT
				time_bucket('1 minute', time) AS minute,
				current_cdn,
				SUM(video_profile_kbps) / 1024 AS traffic_mbps
			FROM content_sessions
			WHERE time > NOW() - INTERVAL '1 hour'
			GROUP BY minute, current_cdn
			ORDER BY minute DESC;
		`)
		if err != nil {
			log.Fatalf("Read query failed: %v", err)
		}
		rows.Close()

	} else { // scylla
		cluster := gocql.NewCluster(config.ScyllaHosts...)
		cluster.Keyspace = config.Keyspace
		s, err := cluster.CreateSession()
		if err != nil {
			log.Fatalf("Read query failed to connect: %v", err)
		}
		defer s.Close()
		_ = s.Query("SELECT current_cdn, video_profile_kbps FROM content_sessions WHERE time > ?", time.Now().Add(-1*time.Hour)).Iter()
	}

	log.Printf("Read query completed in: %v\n", time.Since(startTime))
}
