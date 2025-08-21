package main

import (
	"context"
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

const (
	runDuration  = 15 * time.Minute
	workerCount  = 100
	readInterval = 10 * time.Second
	keyspace     = "content_steering"
	batchSize    = 100 // Increased batch size for higher throughput
)

var (
	cdns      = []string{"Akamai", "Fastly", "Cloudfront", "Qwilt", "NetSkrt"}
	platforms = []string{"mobile", "dotcom", "tv"}
	queryASNs = []int{3356, 15169, 7922, 20115, 8121}
)

type SessionData struct {
	SessionID uuid.UUID
	Time      time.Time
	Platform  string
	ASN       int
	CDN       string
	Kbps      int
}

const insertWriteQuery = `INSERT INTO content_sessions (session_id, time, platform, asn, current_cdn, video_profile_kbps) VALUES (?, ?, ?, ?, ?, ?)`
const insertQueryTableQuery = `INSERT INTO sessions_by_asn (session_id, time, platform, asn, current_cdn, video_profile_kbps) VALUES (?, ?, ?, ?, ?, ?)`

func main() {
	scyllaHosts := os.Getenv("SCYLLA_HOSTS")
	scyllaUser := os.Getenv("SCYLLA_USER")
	scyllaPass := os.Getenv("SCYLLA_PASS")

	if scyllaHosts == "" || scyllaUser == "" || scyllaPass == "" {
		log.Fatal("SCYLLA_HOSTS, SCYLLA_USER, and SCYLLA_PASS environment variables must be set")
	}

	cluster := gocql.NewCluster(strings.Split(scyllaHosts, ",")...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.LocalQuorum
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: scyllaUser,
		Password: scyllaPass,
	}
	cluster.NumConns = 10
	cluster.Compressor = &gocql.SnappyCompressor{}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to ScyllaDB: %v", err)
	}
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), runDuration)
	defer cancel()

	var wg sync.WaitGroup
	var periodicWriteCounter uint64
	var totalWriteCounter uint64

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go writeWorker(ctx, &wg, session, &periodicWriteCounter, &totalWriteCounter, i)
	}

	wg.Add(1)
	go readWorker(ctx, &wg, session)

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				currentWrites := atomic.SwapUint64(&periodicWriteCounter, 0)
				log.Printf("[Monitor] Writes/sec in last 30s: %.2f", float64(currentWrites)/30.0)
			}
		}
	}()

	wg.Wait()

	writesPerSecond := float64(totalWriteCounter) / runDuration.Seconds()
	log.Println("--------------------------------------------------")
	log.Printf("(Dual Write) Load test finished.")
	log.Printf("Total writes to each table: %d", totalWriteCounter)
	log.Printf("Final average ingest rate: %.2f writes/second\n", writesPerSecond)
	log.Println("--------------------------------------------------")
}

func writeWorker(ctx context.Context, wg *sync.WaitGroup, session *gocql.Session, periodicCounter *uint64, totalCounter *uint64, workerID int) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			writeBatch := session.NewBatch(gocql.UnloggedBatch)
			queryBatch := session.NewBatch(gocql.UnloggedBatch)

			for i := 0; i < batchSize; i++ {
				data := SessionData{
					SessionID: uuid.New(),
					Time:      time.Now(),
					Platform:  platforms[rand.Intn(len(platforms))],
					CDN:       cdns[rand.Intn(len(cdns))],
					ASN:       rand.Intn(65000),
					Kbps:      rand.Intn(15000) + 500,
				}
				writeBatch.Query(insertWriteQuery, data.SessionID.String(), data.Time, data.Platform, data.ASN, data.CDN, data.Kbps)
				queryBatch.Query(insertQueryTableQuery, data.SessionID.String(), data.Time, data.Platform, data.ASN, data.CDN, data.Kbps)
			}

			var batchWg sync.WaitGroup
			batchWg.Add(2)
			var batchErr error

			go func() {
				defer batchWg.Done()
				if err := session.ExecuteBatch(writeBatch); err != nil {
					batchErr = err
				}
			}()
			go func() {
				defer batchWg.Done()
				if err := session.ExecuteBatch(queryBatch); err != nil {
					batchErr = err
				}
			}()
			batchWg.Wait()

			if batchErr != nil {
				log.Printf("Worker %d batch insert error: %v", workerID, batchErr)
			} else {
				atomic.AddUint64(periodicCounter, batchSize)
				atomic.AddUint64(totalCounter, batchSize)
			}
		}
	}
}

func readWorker(ctx context.Context, wg *sync.WaitGroup, session *gocql.Session) {
	defer wg.Done()
	ticker := time.NewTicker(readInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			queryASN := queryASNs[rand.Intn(len(queryASNs))]
			startTime := time.Now()
			lookbackTime := time.Now().Add(-1 * time.Hour)

			iter := session.Query(
				`SELECT current_cdn, video_profile_kbps FROM sessions_by_asn WHERE asn = ? AND time > ?`,
				queryASN, lookbackTime).Iter()

			cdnTraffic := make(map[string]int)
			var cdn string
			var kbps int
			rowCount := 0
			for iter.Scan(&cdn, &kbps) {
				cdnTraffic[cdn] += kbps
				rowCount++
			}
			if err := iter.Close(); err != nil {
				log.Printf("Read query error: %v", err)
			} else {
				latency := time.Since(startTime)
				log.Printf("[Read Query] Latency: %v. Scanned %d rows. Aggregated traffic for ASN %d: %v", latency, rowCount, queryASN, cdnTraffic)
			}
		}
	}
}
