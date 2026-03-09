package db

import (
	"fmt"
	"os"
)

type HealthReport struct {
	MemoriesTotal      int `json:"memories_total"`
	VecMemoriesTotal   int `json:"vec_memories_total"`
	MemoriesMissing    int `json:"memories_missing"`
	UserMemoriesTotal  int `json:"user_memories_total"`
	VecUserMemTotal    int `json:"vec_user_memories_total"`
	UserMemoriesMissing int `json:"user_memories_missing"`
	OrphanVec          int `json:"orphan_vec"`
	OrphanUserVec      int `json:"orphan_user_vec"`
}

type DBStats struct {
	FileSizeBytes    int64             `json:"file_size_bytes"`
	FileSizeMB       float64           `json:"file_size_mb"`
	DBPath           string            `json:"db_path"`
	TableCounts      map[string]int    `json:"table_counts"`
	EmbeddingDim     int               `json:"embedding_dim"`
	ProjectDistrib   map[string]int    `json:"project_distribution"`
	ScopeDistrib     map[string]int    `json:"scope_distribution"`
}

func (d *DB) HealthCheck() (*HealthReport, error) {
	r := &HealthReport{}

	d.QueryRow("SELECT COUNT(*) FROM memories").Scan(&r.MemoriesTotal)
	d.QueryRow("SELECT COUNT(*) FROM vec_memories").Scan(&r.VecMemoriesTotal)
	d.QueryRow("SELECT COUNT(*) FROM memories WHERE id NOT IN (SELECT id FROM vec_memories)").Scan(&r.MemoriesMissing)

	d.QueryRow("SELECT COUNT(*) FROM user_memories").Scan(&r.UserMemoriesTotal)
	d.QueryRow("SELECT COUNT(*) FROM vec_user_memories").Scan(&r.VecUserMemTotal)
	d.QueryRow("SELECT COUNT(*) FROM user_memories WHERE id NOT IN (SELECT id FROM vec_user_memories)").Scan(&r.UserMemoriesMissing)

	d.QueryRow("SELECT COUNT(*) FROM vec_memories WHERE id NOT IN (SELECT id FROM memories)").Scan(&r.OrphanVec)
	d.QueryRow("SELECT COUNT(*) FROM vec_user_memories WHERE id NOT IN (SELECT id FROM user_memories)").Scan(&r.OrphanUserVec)

	return r, nil
}

func (d *DB) GetDBStats(dbPath string) (*DBStats, error) {
	stats := &DBStats{
		DBPath:         dbPath,
		TableCounts:    map[string]int{},
		ProjectDistrib: map[string]int{},
		ScopeDistrib:   map[string]int{},
	}

	// File size
	if info, err := os.Stat(dbPath); err == nil {
		stats.FileSizeBytes = info.Size()
		stats.FileSizeMB = float64(info.Size()) / 1024 / 1024
	}

	// Table counts
	tables := []string{"memories", "user_memories", "vec_memories", "vec_user_memories",
		"issues", "issues_archive", "tasks", "tasks_archive", "session_state"}
	for _, t := range tables {
		var count int
		d.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", t)).Scan(&count)
		stats.TableCounts[t] = count
	}

	// Embedding dimension
	var embBytes []byte
	err := d.QueryRow("SELECT embedding FROM vec_memories LIMIT 1").Scan(&embBytes)
	if err == nil && len(embBytes) >= 4 {
		stats.EmbeddingDim = len(embBytes) / 4
	}

	// Project distribution
	rows, err := d.Query("SELECT project_dir, COUNT(*) FROM memories GROUP BY project_dir")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var pd string
			var cnt int
			rows.Scan(&pd, &cnt)
			if pd != "" {
				stats.ProjectDistrib[pd] = cnt
			}
		}
	}

	// Scope distribution
	rows2, err := d.Query("SELECT scope, COUNT(*) FROM memories GROUP BY scope")
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var scope string
			var cnt int
			rows2.Scan(&scope, &cnt)
			stats.ScopeDistrib[scope] = cnt
		}
	}
	// Add user memories count
	var userCount int
	d.QueryRow("SELECT COUNT(*) FROM user_memories").Scan(&userCount)
	if userCount > 0 {
		stats.ScopeDistrib["user"] = userCount
	}

	return stats, nil
}

// GetMissingEmbeddingIDs returns memory IDs that lack embedding vectors
func (d *DB) GetMissingEmbeddingIDs() (projectIDs, userIDs []string, err error) {
	rows, err := d.Query("SELECT id FROM memories WHERE id NOT IN (SELECT id FROM vec_memories)")
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		rows.Scan(&id)
		projectIDs = append(projectIDs, id)
	}

	rows2, err := d.Query("SELECT id FROM user_memories WHERE id NOT IN (SELECT id FROM vec_user_memories)")
	if err != nil {
		return projectIDs, nil, err
	}
	defer rows2.Close()
	for rows2.Next() {
		var id string
		rows2.Scan(&id)
		userIDs = append(userIDs, id)
	}

	return projectIDs, userIDs, nil
}

// GetMemoryContent returns content for embedding generation
func (d *DB) GetMemoryContent(id string) (string, string, error) {
	var content string
	err := d.QueryRow("SELECT content FROM memories WHERE id=?", id).Scan(&content)
	if err == nil {
		return content, "memories", nil
	}
	err = d.QueryRow("SELECT content FROM user_memories WHERE id=?", id).Scan(&content)
	if err == nil {
		return content, "user_memories", nil
	}
	return "", "", fmt.Errorf("memory not found: %s", id)
}

// InsertEmbedding inserts a vector for a memory
func (d *DB) InsertEmbedding(id string, table string, embeddingJSON string) error {
	vecTable := "vec_memories"
	if table == "user_memories" {
		vecTable = "vec_user_memories"
	}
	_, err := d.Exec(fmt.Sprintf("INSERT OR REPLACE INTO %s (id, embedding) VALUES (?,?)", vecTable), id, embeddingJSON)
	return err
}
