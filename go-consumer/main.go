package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type SessionData struct {
	Time             time.Time `json:"time"`
	SessionID        string    `json:"session_id"`
	Platform         string    `json:"platform"`
	Channel          string    `json:"channel"`
	ASN              int       `json:"asn"`
	CurrentCDN       string    `json:"current_cdn"`
	VideoProfileKbps int       `json:"video_profile_kbps"`
}

var (
	brokers = strings.Split(os.Getenv("KAFKA_BROKER"), ",")
	topic   = "content_sessions"
	dbPool  *pgxpool.Pool
)

func main() {
	dbURL := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s",
		os.Getenv("DB_USER"), os.Getenv("DB_PASS"), os.Getenv("DB_HOST"), "30082", os.Getenv("DB_NAME"))

	var err error
	dbPool, err = pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(context.Background()); err != nil {
		log.Fatalf("Unable to ping database: %v\n", err)
	}
	log.Println("Successfully connected to TimescaleDB!")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  "content-steering-group",
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  100 * time.Millisecond,
	})
	defer reader.Close()

	log.Println("Starting Kafka consumer with batch processing...")

	const batchSize = 10000
	batch := make([]SessionData, 0, batchSize)
	var messagesToCommit []kafka.Message

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		var data SessionData
		if err := json.Unmarshal(m.Value, &data); err != nil {
			log.Printf("Error unmarshalling JSON: %v\n", err)
			continue
		}

		batch = append(batch, data)
		messagesToCommit = append(messagesToCommit, m)

		if len(batch) >= batchSize {
			if err := processBatch(batch); err != nil {
				log.Printf("Error processing batch: %v\n", err)
			} else {
				if err := reader.CommitMessages(context.Background(), messagesToCommit...); err != nil {
					log.Printf("Error committing messages to Kafka: %v\n", err)
				}
				log.Printf("Successfully processed and committed a batch of %d messages.", len(batch))
			}
			batch = make([]SessionData, 0, batchSize)
			messagesToCommit = nil
		}
	}
}

func processBatch(batch []SessionData) error {
	if len(batch) == 0 {
		return nil
	}

	rows := make([][]any, len(batch))
	for i, data := range batch {
		rows[i] = []any{data.Time, data.SessionID, data.Platform, data.Channel, data.ASN, data.CurrentCDN, data.VideoProfileKbps}
	}

	_, err := dbPool.CopyFrom(
		context.Background(),
		pgx.Identifier{"content_sessions"},
		[]string{"time", "session_id", "platform", "channel", "asn", "current_cdn", "video_profile_kbps"},
		pgx.CopyFromRows(rows),
	)

	return err
}
