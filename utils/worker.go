package utils

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ClaimNextQueued(ctx context.Context, db *pgxpool.Pool, workerID string) (id, ownerKey, topic string, found bool, err error) {
	row := db.QueryRow(ctx, `
		WITH picked AS (
			SELECT id
			FROM idea_requests
			WHERE status = 'queued'
			ORDER BY created_at ASC
			LIMIT 1
		)
		UPDATE idea_requests ir
		SET status = 'processing',
		    started_at = COALESCE(started_at, now()),
		    locked_at = now(),
		    locked_by = $1
		FROM picked
		WHERE ir.id = picked.id
		  AND ir.status = 'queued'
		RETURNING ir.id::text, ir.owner_key, ir.topic
	`, workerID)

	if scanErr := row.Scan(&id, &ownerKey, &topic); scanErr != nil {
		if scanErr == pgx.ErrNoRows {
			return "", "", "", false, nil
		}
		return "", "", "", false, scanErr
	}

	return id, ownerKey, topic, true, nil
}

func MarkSucceeded(ctx context.Context, db *pgxpool.Pool, id string) error {
	_, err := db.Exec(ctx, `
		UPDATE idea_requests
		SET status='succeeded',
		    finished_at=now()
		WHERE id=$1
	`, id)
	return err
}

func MarkFailed(ctx context.Context, db *pgxpool.Pool, id string) error {
	_, err := db.Exec(ctx, `
		UPDATE idea_requests
		SET status='failed',
		    finished_at=now()
		WHERE id=$1
	`, id)
	return err
}
