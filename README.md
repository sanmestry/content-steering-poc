# Content Steering Proof of Concept

This project demonstrates a real-time data pipeline for a content steering proof of concept using **Go**, **Apache Kafka**, and **TimescaleDB**. It simulates concurrent clients, ingests session data, and stores it in a time-series database for real-time analytics and dashboarding with **Grafana**.

---

## Project Architecture

The project consists of the following components, all orchestrated with Docker Compose:

- **go-producer**:  
  A Go application that simulates client devices pushing session data to a Kafka topic. Highly concurrent and designed for high throughput.

- **go-consumer**:  
  A Go application that reads data from the Kafka topic and writes it to TimescaleDB. Handles data persistence and database schema creation.

- **Apache Kafka & Zookeeper**:  
  A robust, distributed streaming platform that acts as a message bus to decouple the producer from the consumer.

- **TimescaleDB**:  
  A time-series database built on PostgreSQL, optimized for fast data ingestion and complex time-based queries.

- **Grafana**:  
  An analytics and visualization platform used to build real-time dashboards to monitor key metrics.

> The entire stack is configured to be run locally in a single command.

---

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### Setup

Clone the repository:

```bash
git clone https://github.com/sanmestry/content-steering-poc.git
cd content-steering-poc
```

### Start the services:

```bash
docker-compose up --build
```

This command will build the custom Go applications, pull the other required images (Kafka, TimescaleDB, Grafana), and start all services.

#### Access Grafana:
Open your web browser and navigate to http://localhost:3000. Log in with the default credentials: admin / admin.

#### Go Producer (go-producer/main.go)
This application simulates real-world client traffic. It generates session data with fields like session_id, asn, current_cdn, and video_profile_kbps and publishes it to the content_sessions Kafka topic.

#### Go Consumer (go-consumer/main.go)
This service consumes messages from the Kafka topic and inserts them into the content_sessions hypertable in TimescaleDB. It's designed to be robust and handle the high volume of incoming data.

#### Database Initialization (db/init.sql)
This SQL script is automatically executed on startup to create the necessary tables, including the content_sessions hypertable and supporting tables for CDN priority and capacity.

#### Grafana Provisioning (grafana/provisioning)
The project includes pre-configured provisioning files to automatically set up the TimescaleDB data source and a default dashboard. The dashboards visualize metrics such as:

- Real-time traffic distribution by CDN.
- Real-time traffic distribution by ASN.

#### Dashboard Queries
The provided Grafana dashboards use these efficient SQL queries to visualize the data:

#### Real-time Traffic by CDN:


```SQL
SELECT
time_bucket('1 minute', time) AS "time",
current_cdn,
(sum(video_profile_kbps) / 1024) AS "traffic_mbps"
FROM content_sessions
WHERE
time BETWEEN $__timeFrom() AND $__timeTo()
GROUP BY time_bucket('1 minute', time), current_cdn
ORDER BY "time";
Real-time Traffic by ASN:
```

```SQL
SELECT
time_bucket('1 minute', time) AS "time",
asn,
(sum(video_profile_kbps) / 1024) AS "traffic_mbps"
FROM content_sessions
WHERE
time BETWEEN $__timeFrom() AND $__timeTo()
GROUP BY time_bucket('1 minute', time), asn
ORDER BY "time";
```

### Stopping the Services
To stop the services, run:

```bash
docker-compose down
```

### Conclusion
This PoC shows that a Kafka-TimescaleDB-Grafana stack is a solid, scalable solution for real-time data.


* High-Volume Traffic: The system can handle large data points per minute.
* Reliability: Kafka acts as a buffer, preventing the database from crashing under heavy load.
* Performance: TimescaleDB handles all the time-stamped data and complex queries quickly.
* Live Dashboards: Grafana builds live dashboards so you can see what's happening in real-time.
* It proves this tech stack can reliably handle huge amounts of data.
