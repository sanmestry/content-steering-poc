package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

// --- Configuration ---
const (
	runDuration  = 15 * time.Minute // How long to run the test
	workerCount  = 100              // Number of concurrent write clients
	readInterval = 10 * time.Second // How often to run a read query
	keyspace     = "content_steering"
	tableName    = "content_sessions"
)

var (
	// Use the expanded list of CDNs and new platforms
	cdns      = []string{"Akamai", "Fastly", "Cloudfront", "Qwilt", "NetSkrt"}
	platforms = []string{"mobile", "dotcom", "tv"}
)

// SessionData simulates your session data
type SessionData struct {
	SessionID uuid.UUID
	Time      time.Time
	Platform  string
	ASN       int
	CDN       string
	Kbps      int
}

func main() {
	scyllaHosts := os.Getenv("SCYLLA_HOSTS")
	if scyllaHosts == "" {
		log.Fatal("SCYLLA_HOSTS environment variable not set (e.g., '127.0.0.1')")
	}

	log.Printf("Starting ScyllaDB load test for %v...", runDuration)
	log.Printf("Workers: %d, Read Query Interval: %v", workerCount, readInterval)

	// Create a ScyllaDB session
	cluster := gocql.NewCluster(strings.Split(scyllaHosts, ",")...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.LocalQuorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to ScyllaDB: %v", err)
	}
	defer session.Close()

	// Use a context to control the run duration
	ctx, cancel := context.WithTimeout(context.Background(), runDuration)
	defer cancel()

	var wg sync.WaitGroup
	var writeCounter uint64

	// --- Start Write Workers ---
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go writeWorker(ctx, &wg, session, writeCounter, i)
	}

	// --- Start Read Worker ---
	wg.Add(1)
	go readWorker(ctx, &wg, session)

	// --- Wait for test to finish ---
	wg.Wait()

	// --- Print Final Results ---
	totalWrites := atomic.LoadUint64(&writeCounter)
	writesPerSecond := float64(totalWrites) / runDuration.Seconds()
	log.Println("--------------------------------------------------")
	log.Printf("Load test finished.")
	log.Printf("Total writes: %d", totalWrites)
	log.Printf("Average ingest rate: %.2f writes/second\n", writesPerSecond)
	log.Println("--------------------------------------------------")
}

// writeWorker simulates a single client continuously writing data
func writeWorker(ctx context.Context, wg *sync.WaitGroup, session *gocql.Session, writeCounter uint64, workerID int) {
	defer wg.Done()
	log.Printf("Write worker %d started", workerID)

	for {
		select {
		case <-ctx.Done(): // Stop when the context is cancelled
			log.Printf("Write worker %d stopping", workerID)
			return
		default:
			data := SessionData{
				SessionID: uuid.New(),
				Time:      time.Now(),
				Platform:  platforms[rand.Intn(len(platforms))],
				CDN:       cdns[rand.Intn(len(cdns))],
				ASN:       rand.Intn(65000),
				Kbps:      rand.Intn(15000) + 500,
			}

			err := session.Query(fmt.Sprintf(
				`INSERT INTO %s (session_id, time, platform, asn, current_cdn, video_profile_kbps) VALUES (?, ?, ?, ?, ?, ?)`,
				tableName),
				data.SessionID, data.Time, data.Platform, data.ASN, data.CDN, data.Kbps).Exec()

			if err != nil {
				log.Printf("Worker %d write error: %v", workerID, err)
			} else {
				atomic.AddUint64(&writeCounter, 1)
			}
			time.Sleep(10 * time.Millisecond) // Small delay to prevent overwhelming a single worker
		}
	}
}

// readWorker periodically runs aggregation-style queries
func readWorker(ctx context.Context, wg *sync.WaitGroup, session *gocql.Session) {
	defer wg.Done()
	log.Println("Read worker started")
	ticker := time.NewTicker(readInterval)

	for {
		select {
		case <-ctx.Done():
			log.Println("Read worker stopping")
			return
		case <-ticker.C:
			// ScyllaDB does not support server-side GROUP BY. The strategy is to
			// fetch the required data and perform the aggregation client-side.
			// This query simulates fetching data for a specific, high-traffic ASN.
			queryASN := 3356 // Example ASN

			startTime := time.Now()
			iter := session.Query(fmt.Sprintf(
				`SELECT current_cdn, video_profile_kbps FROM %s WHERE asn = ? AND time > ? ALLOW FILTERING`,
				tableName),
				queryASN, time.Now().Add(-1*time.Hour)).Iter()

			// Client-side aggregation
			cdnTraffic := make(map[string]int)
			var cdn string
			var kbps int
			for iter.Scan(&cdn, &kbps) {
				cdnTraffic[cdn] += kbps
			}

			if err := iter.Close(); err != nil {
				log.Printf("Read query error: %v", err)
			} else {
				latency := time.Since(startTime)
				log.Printf("[Read Query] Latency: %v. Aggregated traffic for ASN %d: %v", latency, queryASN, cdnTraffic)
			}
		}
	}
}
