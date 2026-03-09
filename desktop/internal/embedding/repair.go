package embedding

import (
	"context"
	"encoding/json"
	"fmt"

	"desktop/internal/db"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

type RepairProgress struct {
	Current int    `json:"current"`
	Total   int    `json:"total"`
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
}

// BatchRepair fixes memories missing embeddings, emitting progress events
func BatchRepair(ctx context.Context, d *db.DB, engine *Engine, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 50
	}

	projectIDs, userIDs, err := d.GetMissingEmbeddingIDs()
	if err != nil {
		return err
	}

	allIDs := append(projectIDs, userIDs...)
	total := len(allIDs)
	if total == 0 {
		runtime.EventsEmit(ctx, "repair:progress", RepairProgress{
			Current: 0, Total: 0, Status: "done",
		})
		return nil
	}

	for i, id := range allIDs {
		content, table, err := d.GetMemoryContent(id)
		if err != nil {
			continue
		}

		emb, err := engine.Encode(content)
		if err != nil {
			runtime.EventsEmit(ctx, "repair:progress", RepairProgress{
				Current: i + 1, Total: total, Status: "error",
				Error: fmt.Sprintf("encode %s: %v", id, err),
			})
			continue
		}

		embJSON, _ := json.Marshal(emb)
		if err := d.InsertEmbedding(id, table, string(embJSON)); err != nil {
			continue
		}

		runtime.EventsEmit(ctx, "repair:progress", RepairProgress{
			Current: i + 1, Total: total, Status: "running",
		})
	}

	runtime.EventsEmit(ctx, "repair:progress", RepairProgress{
		Current: total, Total: total, Status: "done",
	})
	return nil
}

// RebuildAllEmbeddings regenerates all embeddings (async via goroutine)
func RebuildAllEmbeddings(ctx context.Context, d *db.DB, engine *Engine) {
	go func() {
		// Get all memory IDs
		var allIDs []struct {
			ID    string
			Table string
		}

		rows, err := d.Query("SELECT id FROM memories")
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id string
				rows.Scan(&id)
				allIDs = append(allIDs, struct {
					ID    string
					Table string
				}{id, "memories"})
			}
		}

		rows2, err := d.Query("SELECT id FROM user_memories")
		if err == nil {
			defer rows2.Close()
			for rows2.Next() {
				var id string
				rows2.Scan(&id)
				allIDs = append(allIDs, struct {
					ID    string
					Table string
				}{id, "user_memories"})
			}
		}

		total := len(allIDs)
		for i, item := range allIDs {
			content, _, err := d.GetMemoryContent(item.ID)
			if err != nil {
				continue
			}

			emb, err := engine.Encode(content)
			if err != nil {
				runtime.EventsEmit(ctx, "rebuild:progress", RepairProgress{
					Current: i + 1, Total: total, Status: "error",
					Error: fmt.Sprintf("encode %s: %v", item.ID, err),
				})
				continue
			}

			embJSON, _ := json.Marshal(emb)
			d.InsertEmbedding(item.ID, item.Table, string(embJSON))

			runtime.EventsEmit(ctx, "rebuild:progress", RepairProgress{
				Current: i + 1, Total: total, Status: "running",
			})
		}

		runtime.EventsEmit(ctx, "rebuild:progress", RepairProgress{
			Current: total, Total: total, Status: "done",
		})
	}()
}
