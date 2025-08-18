package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// SessionData represents the updated structure of the JSON message from Kafka
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
		os.Getenv("DB_USER"), os.Getenv("DB_PASS"), os.Getenv("DB_HOST"), "5432", os.Getenv("DB_NAME"))

	var err error
	dbPool, err = pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}
	defer dbPool.Close()

	// Ping the database to ensure connection is established
	if err := dbPool.Ping(context.Background()); err != nil {
		log.Fatalf("Unable to ping database: %v\n", err)
	}
	log.Println("Successfully connected to TimescaleDB!")

	// Set up Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  "content-steering-group",
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  100 * time.Millisecond,
	})
	defer reader.Close()

	log.Println("Starting Kafka consumer...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		// Unmarshal the JSON data
		var data SessionData
		if err := json.Unmarshal(m.Value, &data); err != nil {
			log.Printf("Error unmarshalling JSON: %v\n", err)
			continue
		}

		// Insert into TimescaleDB using the connection pool
		if _, err := dbPool.Exec(context.Background(),
			`INSERT INTO content_sessions (time, session_id, platform, channel, asn, current_cdn, video_profile_kbps) 
            VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			data.Time, data.SessionID, data.Platform, data.Channel, data.ASN, data.CurrentCDN, data.VideoProfileKbps,
		); err != nil {
			log.Printf("Error inserting into TimescaleDB: %v\n", err)
		}

		log.Printf("Successfully inserted message from topic %s, partition %d, offset %d\n", m.Topic, m.Partition, m.Offset)
	}
}
