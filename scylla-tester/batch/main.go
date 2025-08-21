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
	workerCount  = 1
	readInterval = 10 * time.Second
	keyspace     = "content_steering"
	batchSize    = 20 // Batch size for inserts
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

const insertQuery = `INSERT INTO content_sessions (session_id, time, platform, asn, current_cdn, video_profile_kbps) VALUES (?, ?, ?, ?, ?, ?)`

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
	cluster.NumConns = 10                          // Connection pooling
	cluster.Compressor = &gocql.SnappyCompressor{} // Enable compression

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to ScyllaDB: %v", err)
	}
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), runDuration)
	defer cancel()

	var wg sync.WaitGroup
	var writeCounter uint64

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go writeWorker(ctx, &wg, session, &writeCounter, i)
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
				log.Printf("[Monitor] Writes: %d", atomic.LoadUint64(&writeCounter))
			}
		}
	}()

	wg.Wait()

	totalWrites := atomic.LoadUint64(&writeCounter)
	writesPerSecond := float64(totalWrites) / runDuration.Seconds()
	log.Println("--------------------------------------------------")
	log.Printf("(Batching Supported) Load test finished.")
	log.Printf("Total writes: %d", totalWrites)
	log.Printf("Average ingest rate: %.2f writes/second\n", writesPerSecond)
	log.Println("--------------------------------------------------")
}

func writeWorker(ctx context.Context, wg *sync.WaitGroup, session *gocql.Session, writeCounter *uint64, workerID int) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			batch := session.NewBatch(gocql.UnloggedBatch)
			for i := 0; i < batchSize; i++ {
				data := SessionData{
					SessionID: uuid.New(),
					Time:      time.Now(),
					Platform:  platforms[rand.Intn(len(platforms))],
					CDN:       cdns[rand.Intn(len(cdns))],
					ASN:       rand.Intn(65000),
					Kbps:      rand.Intn(15000) + 500,
				}
				batch.Query(insertQuery, data.SessionID.String(), data.Time, data.Platform, data.ASN, data.CDN, data.Kbps)
			}
			if err := session.ExecuteBatch(batch); err != nil {
				log.Printf("Worker %d batch insert error: %v", workerID, err)
			} else {
				atomic.AddUint64(writeCounter, batchSize)
			}
			time.Sleep(10 * time.Millisecond)
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
			lookbackTime := time.Now().Add(-24 * time.Hour)

			iter := session.Query(
				`SELECT current_cdn, video_profile_kbps FROM content_sessions WHERE asn = ? AND time > ? ALLOW FILTERING`,
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
