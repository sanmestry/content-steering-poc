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
	cdns      = []string{"Akamai", "Fastly", "Cloudfront", "Qwilt", "NetSkrt"}
	platforms = []string{"mobile", "dotcom", "tv"}
	// NEW: A list of ASNs to query for in the read worker
	queryASNs = []int{3356, 15169, 7922, 20115, 8121}
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
	// --- Read all connection details from environment variables ---
	scyllaHosts := os.Getenv("SCYLLA_HOSTS")
	scyllaUser := os.Getenv("SCYLLA_USER")
	scyllaPass := os.Getenv("SCYLLA_PASS")

	if scyllaHosts == "" || scyllaUser == "" || scyllaPass == "" {
		log.Fatal("SCYLLA_HOSTS, SCYLLA_USER, and SCYLLA_PASS environment variables must be set")
	}

	log.Printf("Starting ScyllaDB load test for %v...", runDuration)
	log.Printf("Workers: %d, Read Query Interval: %v", workerCount, readInterval)

	// --- Configure the cluster connection ---
	cluster := gocql.NewCluster(strings.Split(scyllaHosts, ",")...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.LocalQuorum

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: scyllaUser,
		Password: scyllaPass,
	}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to ScyllaDB: %v", err)
	}
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), runDuration)
	defer cancel()

	var wg sync.WaitGroup
	var writeCounter uint64

	// Start Write Workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go writeWorker(ctx, &wg, session, writeCounter, i)
	}

	// Start Read Worker
	wg.Add(1)
	go readWorker(ctx, &wg, session)

	wg.Wait()

	// Print Final Results
	totalWrites := atomic.LoadUint64(&writeCounter)
	writesPerSecond := float64(totalWrites) / runDuration.Seconds()
	log.Println("--------------------------------------------------")
	log.Printf("Load test finished.")
	log.Printf("Total writes: %d", totalWrites)
	log.Printf("Average ingest rate: %.2f writes/second\n", writesPerSecond)
	log.Println("--------------------------------------------------")
}

func writeWorker(ctx context.Context, wg *sync.WaitGroup, session *gocql.Session, writeCounter uint64, workerID int) {
	defer wg.Done()
	log.Printf("Write worker %d started", workerID)

	for {
		select {
		case <-ctx.Done():
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
				data.SessionID.String(), data.Time, data.Platform, data.ASN, data.CDN, data.Kbps).Exec()

			if err != nil {
				log.Printf("Worker %d write error: %v", workerID, err)
			} else {
				atomic.AddUint64(&writeCounter, 1)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// readWorker periodically runs aggregation-style queries against a random ASN
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
			// MODIFIED: Randomly pick an ASN from the list for each query
			queryASN := queryASNs[rand.Intn(len(queryASNs))]

			startTime := time.Now()
			iter := session.Query(fmt.Sprintf(
				`SELECT current_cdn, video_profile_kbps FROM %s WHERE asn = ? AND time > ? ALLOW FILTERING`,
				tableName),
				queryASN, time.Now().Add(-1*time.Hour)).Iter()

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
