package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/openai/openai-go/v3"
)

type GeneratedIdea struct {
	Title  string `json:"title"`
	Hook   string `json:"hook"`
	Script string `json:"script"`
	CTA    string `json:"cta"`
}

func GenerateIdeas(ctx context.Context, client *openai.Client, topic string) ([]GeneratedIdea, error) {
	system := `You generate social media content ideas.
  Return ONLY valid JSON (no markdown).
  Output must be a JSON array of exactly 10 objects.
  Each object must have: title, hook, script, cta.
  Keep each idea distinct and practical.`
	user := fmt.Sprintf("Topic: %s", topic)

	resp, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: openai.ChatModelGPT4o, // gyors+jó default az SDK példái alapján
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(system),
			openai.UserMessage(user),
		},
		// Opcionális: kicsit determinisztikusabb
		Temperature: openai.Float(0.8),
	})
	if err != nil {
		return nil, err
	}

	raw := resp.Choices[0].Message.Content

	// néha előfordul, hogy whitespace vagy extra szöveg jön -> minimal sanitize
	raw = strings.TrimSpace(raw)

	var ideas []GeneratedIdea
	if err := json.Unmarshal([]byte(raw), &ideas); err != nil {
		return nil, fmt.Errorf("failed to parse model JSON: %w; raw=%q", err, raw)
	}
	if len(ideas) != 10 {
		return nil, fmt.Errorf("expected 10 ideas, got %d", len(ideas))
	}
	return ideas, nil
}

func InsertIdeas(ctx context.Context, pool *pgxpool.Pool, requestID string, ideas []GeneratedIdea) error {
	batch := &pgx.Batch{}
	for i, it := range ideas {
		batch.Queue(`
		insert into idea_outputs (request_id, idx, title, hook, script, cta)
		values ($1, $2, $3, $4, $5, $6)
		on conflict (request_id, idx) do nothing
	  `, requestID, i, it.Title, it.Hook, it.Script, it.CTA)
	}

	br := pool.SendBatch(ctx, batch)
	defer br.Close()

	for range ideas {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}
