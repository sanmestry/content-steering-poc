package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

var (
	brokers = strings.Split(os.Getenv("KAFKA_BROKER"), ",")
	topic   = "content_sessions"
)

// SessionData represents the structure of the JSON message
type SessionData struct {
	Time             time.Time `json:"time"`
	SessionID        string    `json:"session_id"`
	Platform         string    `json:"platform"`
	Channel          string    `json:"channel"`
	ASN              int       `json:"asn"`
	CurrentCDN       string    `json:"current_cdn"`
	VideoProfileKbps int       `json:"video_profile_kbps"`
}

func main() {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequiredAcks(1),
		// Use batching for performance
		BatchSize:    100,         // Batch up to 100 messages
		BatchBytes:   10e6,        // or up to 10MB
		BatchTimeout: time.Second, // or every second
	}
	defer writer.Close()

	log.Println("Starting Kafka producer...")

	const recordsPerMinute = 1000000
	const recordsPerSecond = recordsPerMinute / 60
	const totalRecords = recordsPerMinute

	var messages []kafka.Message
	log.Printf("Starting to generate and send %d records...\n", totalRecords)

	for i := 0; i < totalRecords; i++ {
		data := generateSessionData()
		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Printf("Failed to marshal JSON: %v\n", err)
			continue
		}
		messages = append(messages, kafka.Message{Value: jsonData})

		// Send a batch every second
		if len(messages) >= recordsPerSecond {
			if err := writer.WriteMessages(context.Background(), messages...); err != nil {
				log.Printf("Failed to write batch: %v\n", err)
			} else {
				log.Printf("Successfully sent a batch of %d messages.\n", len(messages))
			}
			messages = nil          // Clear the batch for the next second
			time.Sleep(time.Second) // Control the rate
		}
	}

	// Send any remaining messages
	if len(messages) > 0 {
		if err := writer.WriteMessages(context.Background(), messages...); err != nil {
			log.Printf("Failed to write final batch: %v\n", err)
		} else {
			log.Printf("Successfully sent the final batch of %d messages.\n", len(messages))
		}
	}

	log.Println("Producer finished sending messages.")
}

func generateSessionData() SessionData {
	cdns := []string{"Akamai", "Fastly", "CloudFront"}
	platforms := []string{"dotcom", "mobile", "tv"}
	channels := []string{"channel1", "channel2", "channel3"}
	asns := []int{0, 2856, rand.Intn(50000) + 1}

	return SessionData{
		Time:             time.Now().UTC(),
		SessionID:        uuid.New().String(),
		Platform:         platforms[rand.Intn(len(platforms))],
		Channel:          channels[rand.Intn(len(channels))],
		ASN:              asns[rand.Intn(len(asns))],
		CurrentCDN:       cdns[rand.Intn(len(cdns))],
		VideoProfileKbps: rand.Intn(10000) + 500,
	}
}
