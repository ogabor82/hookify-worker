package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"hookify-worker/utils"

	"encoding/json"

	"github.com/nats-io/nats.go"
)

type IdeaClaimedEvent struct {
	RequestID string `json:"request_id"`
	OwnerKey  string `json:"owner_key"`
	Topic     string `json:"topic"`
	ClaimedBy string `json:"claimed_by"`
}

const SubjectIdeaClaimed = "idea.claimed"

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Warning: .env file not found: %v", err)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fmt.Println("DATABASE_URL is missing")
		return
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		fmt.Println("NATS_URL is missing")
		return
	}

	natsConn, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Println("failed to connect to nats: %v", err)
		return
	}
	defer natsConn.Close()

	fmt.Println("Worker ID:", workerID)

	pollEvery := 2 * time.Second
	if v := os.Getenv("POLL_EVERY_MS"); v != "" {
		if ms, err := time.ParseDuration(v + "ms"); err == nil && ms > 0 {
			pollEvery = ms
		}
	}

	fmt.Println("Poll every:", pollEvery)

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		fmt.Println("failed to create db pool: %v", err)
		return
	}
	defer pool.Close()

	// fail fast if DB is unreachable
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		fmt.Println("failed to ping db: %v", err)
		return
	}

	fmt.Println("worker started:", workerID)

	for {
		id, ownerKey, topic, found, err := utils.ClaimNextQueued(ctx, pool, workerID)
		if err != nil {
			fmt.Printf("failed to claim next queued: %v\n", err)
			return
		}
		if !found {
			fmt.Println("no queued idea requests found")
			time.Sleep(pollEvery)
			continue
		}
		fmt.Printf("claimed request id=%s owner=%s topic=%q\n", id, ownerKey, topic)

		event := IdeaClaimedEvent{
			RequestID: id,
			OwnerKey:  ownerKey,
			Topic:     topic,
			ClaimedBy: workerID,
		}

		json, err := json.Marshal(event)
		if err != nil {
			fmt.Printf("failed to marshal event: %v\n", err)
			return
		}

		if err := natsConn.Publish(SubjectIdeaClaimed, json); err != nil {
			fmt.Printf("failed to publish idea.claimed event: %v\n", err)
			// döntés: itt akár continue is lehet, nem kell megállni
		}

		err = utils.MarkSucceeded(ctx, pool, id)
		if err != nil {
			fmt.Printf("failed to mark request as succeeded: %v\n", err)
			return
		}
		fmt.Printf("marked request as succeeded id=%s\n", id)
	}
}
