package db

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

type Memory struct {
	ID        string   `json:"id"`
	Content   string   `json:"content"`
	Tags      []string `json:"tags"`
	Scope     string   `json:"scope,omitempty"`
	Source    string   `json:"source"`
	ProjectDir string  `json:"project_dir,omitempty"`
	SessionID  int     `json:"session_id"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  string  `json:"updated_at"`
	Similarity float64 `json:"similarity,omitempty"`
}

type MemoryListResult struct {
	Memories []Memory `json:"memories"`
	Total    int      `json:"total"`
}

func (d *DB) GetMemories(scope, projectDir, query, tag, source string, limit, offset int) (*MemoryListResult, error) {
	if limit <= 0 {
		limit = 100
	}

	if tag != "" {
		return d.getMemoriesByTag(scope, projectDir, query, tag, source, limit, offset)
	}
	if query != "" {
		return d.getMemoriesByKeyword(scope, projectDir, query, source, limit, offset)
	}
	return d.getMemoriesPaginated(scope, projectDir, source, limit, offset)
}

func (d *DB) getMemoriesPaginated(scope, projectDir, source string, limit, offset int) (*MemoryListResult, error) {
	var memories []Memory
	var total int

	switch scope {
	case "user":
		total = d.countTable("user_memories", "")
		memories = d.queryMemories("SELECT id, content, tags, '' as scope, source, '' as project_dir, session_id, created_at, updated_at FROM user_memories ORDER BY created_at DESC LIMIT ? OFFSET ?", limit, offset)
	case "project":
		total = d.countTableWhere("memories", "project_dir=?", projectDir)
		memories = d.queryMemories("SELECT * FROM memories WHERE project_dir=? ORDER BY created_at DESC LIMIT ? OFFSET ?", projectDir, limit, offset)
	default:
		total = d.countTable("memories", "") + d.countTable("user_memories", "")
		memories = d.queryMemories("SELECT * FROM memories ORDER BY created_at DESC LIMIT ? OFFSET ?", limit, offset)
		if len(memories) < limit {
			userMems := d.queryMemories("SELECT id, content, tags, 'user' as scope, source, '' as project_dir, session_id, created_at, updated_at FROM user_memories ORDER BY created_at DESC LIMIT ?", limit-len(memories))
			memories = append(memories, userMems...)
		}
	}

	if source != "" {
		filtered := make([]Memory, 0)
		for _, m := range memories {
			if m.Source == source {
				filtered = append(filtered, m)
			}
		}
		memories = filtered
	}

	return &MemoryListResult{Memories: memories, Total: total}, nil
}

func (d *DB) getMemoriesByTag(scope, projectDir, query, tag, source string, limit, offset int) (*MemoryListResult, error) {
	var allRows []Memory

	switch scope {
	case "user":
		allRows = d.queryMemories(
			"SELECT um.id, um.content, um.tags, 'user' as scope, um.source, '' as project_dir, um.session_id, um.created_at, um.updated_at FROM user_memories um JOIN user_memory_tags umt ON um.id=umt.memory_id WHERE umt.tag=? ORDER BY um.created_at DESC", tag)
	case "project":
		allRows = d.queryMemories(
			"SELECT m.* FROM memories m JOIN memory_tags mt ON m.id=mt.memory_id WHERE mt.tag=? AND m.project_dir=? ORDER BY m.created_at DESC", tag, projectDir)
	default:
		proj := d.queryMemories(
			"SELECT m.* FROM memories m JOIN memory_tags mt ON m.id=mt.memory_id WHERE mt.tag=? AND m.project_dir=? ORDER BY m.created_at DESC", tag, projectDir)
		user := d.queryMemories(
			"SELECT um.id, um.content, um.tags, 'user' as scope, um.source, '' as project_dir, um.session_id, um.created_at, um.updated_at FROM user_memories um JOIN user_memory_tags umt ON um.id=umt.memory_id WHERE umt.tag=? ORDER BY um.created_at DESC", tag)
		allRows = append(proj, user...)
	}

	if source != "" {
		filtered := make([]Memory, 0)
		for _, m := range allRows {
			if m.Source == source {
				filtered = append(filtered, m)
			}
		}
		allRows = filtered
	}
	if query != "" {
		q := strings.ToLower(query)
		filtered := make([]Memory, 0)
		for _, m := range allRows {
			if strings.Contains(strings.ToLower(m.Content), q) {
				filtered = append(filtered, m)
			}
		}
		allRows = filtered
	}

	total := len(allRows)
	end := offset + limit
	if end > total {
		end = total
	}
	if offset > total {
		offset = total
	}
	return &MemoryListResult{Memories: allRows[offset:end], Total: total}, nil
}

func (d *DB) getMemoriesByKeyword(scope, projectDir, query, source string, limit, offset int) (*MemoryListResult, error) {
	var allRows []Memory
	switch scope {
	case "user":
		allRows = d.queryMemories("SELECT id, content, tags, 'user' as scope, source, '' as project_dir, session_id, created_at, updated_at FROM user_memories ORDER BY created_at DESC")
	case "project":
		allRows = d.queryMemories("SELECT * FROM memories WHERE project_dir=? ORDER BY created_at DESC", projectDir)
	default:
		allRows = d.queryMemories("SELECT * FROM memories ORDER BY created_at DESC")
		userRows := d.queryMemories("SELECT id, content, tags, 'user' as scope, source, '' as project_dir, session_id, created_at, updated_at FROM user_memories ORDER BY created_at DESC")
		allRows = append(allRows, userRows...)
	}

	if source != "" {
		filtered := make([]Memory, 0)
		for _, m := range allRows {
			if m.Source == source {
				filtered = append(filtered, m)
			}
		}
		allRows = filtered
	}

	q := strings.ToLower(query)
	filtered := make([]Memory, 0)
	for _, m := range allRows {
		if strings.Contains(strings.ToLower(m.Content), q) {
			filtered = append(filtered, m)
		}
	}
	allRows = filtered

	total := len(allRows)
	end := offset + limit
	if end > total {
		end = total
	}
	if offset > total {
		offset = total
	}
	return &MemoryListResult{Memories: allRows[offset:end], Total: total}, nil
}

func (d *DB) GetMemoryDetail(id string) (*Memory, error) {
	mems := d.queryMemories("SELECT * FROM memories WHERE id=?", id)
	if len(mems) > 0 {
		return &mems[0], nil
	}
	mems = d.queryMemories("SELECT id, content, tags, 'user' as scope, source, '' as project_dir, session_id, created_at, updated_at FROM user_memories WHERE id=?", id)
	if len(mems) > 0 {
		return &mems[0], nil
	}
	return nil, fmt.Errorf("memory not found: %s", id)
}

func (d *DB) UpdateMemory(id, content string, tags []string, scope string) (*Memory, error) {
	now := time.Now().Format(time.RFC3339)

	// Determine which table
	var table string
	row := d.QueryRow("SELECT id FROM memories WHERE id=?", id)
	var dummy string
	if row.Scan(&dummy) == nil {
		table = "memories"
	} else {
		row = d.QueryRow("SELECT id FROM user_memories WHERE id=?", id)
		if row.Scan(&dummy) == nil {
			table = "user_memories"
		} else {
			return nil, fmt.Errorf("memory not found: %s", id)
		}
	}

	sets := []string{}
	args := []interface{}{}
	if content != "" {
		sets = append(sets, "content=?")
		args = append(args, content)
	}
	if tags != nil {
		tagsJSON, _ := json.Marshal(tags)
		sets = append(sets, "tags=?")
		args = append(args, string(tagsJSON))
	}
	if scope != "" && table == "memories" {
		sets = append(sets, "scope=?")
		args = append(args, scope)
	}
	if len(sets) > 0 {
		sets = append(sets, "updated_at=?")
		args = append(args, now)
		args = append(args, id)
		_, err := d.Exec(fmt.Sprintf("UPDATE %s SET %s WHERE id=?", table, strings.Join(sets, ",")), args...)
		if err != nil {
			return nil, err
		}
	}

	return d.GetMemoryDetail(id)
}

func (d *DB) DeleteMemory(id string) error {
	// Try project memories first
	res, _ := d.Exec("DELETE FROM memories WHERE id=?", id)
	if n, _ := res.RowsAffected(); n > 0 {
		d.Exec("DELETE FROM vec_memories WHERE id=?", id)
		d.Exec("DELETE FROM memory_tags WHERE memory_id=?", id)
		return nil
	}
	// Try user memories
	res, _ = d.Exec("DELETE FROM user_memories WHERE id=?", id)
	if n, _ := res.RowsAffected(); n > 0 {
		d.Exec("DELETE FROM vec_user_memories WHERE id=?", id)
		d.Exec("DELETE FROM user_memory_tags WHERE memory_id=?", id)
		return nil
	}
	return fmt.Errorf("memory not found: %s", id)
}

func (d *DB) DeleteMemoriesBatch(ids []string) (int, error) {
	deleted := 0
	for _, id := range ids {
		if err := d.DeleteMemory(id); err == nil {
			deleted++
		}
	}
	return deleted, nil
}

// Export memories with embedding data
type MemoryExport struct {
	Memory
	Embedding []float32 `json:"embedding"`
}

func (d *DB) ExportMemories(scope, projectDir string) ([]MemoryExport, error) {
	var memories []Memory
	switch scope {
	case "user":
		memories = d.queryMemories("SELECT id, content, tags, 'user' as scope, source, '' as project_dir, session_id, created_at, updated_at FROM user_memories")
	case "project":
		memories = d.queryMemories("SELECT * FROM memories WHERE project_dir=?", projectDir)
	default:
		memories = d.queryMemories("SELECT * FROM memories")
		userMems := d.queryMemories("SELECT id, content, tags, 'user' as scope, source, '' as project_dir, session_id, created_at, updated_at FROM user_memories")
		memories = append(memories, userMems...)
	}

	result := make([]MemoryExport, 0, len(memories))
	for _, m := range memories {
		export := MemoryExport{Memory: m}
		vecTable := "vec_memories"
		if m.Scope == "user" || m.ProjectDir == "" {
			vecTable = "vec_user_memories"
		}
		row := d.QueryRow(fmt.Sprintf("SELECT embedding FROM %s WHERE id=?", vecTable), m.ID)
		var embBytes []byte
		if row.Scan(&embBytes) == nil && len(embBytes) >= 4 && len(embBytes)%4 == 0 {
			count := len(embBytes) / 4
			emb := make([]float32, count)
			for i := 0; i < count; i++ {
				bits := uint32(embBytes[i*4]) | uint32(embBytes[i*4+1])<<8 | uint32(embBytes[i*4+2])<<16 | uint32(embBytes[i*4+3])<<24
				emb[i] = float32FromBits(bits)
			}
			export.Embedding = emb
		}
		result = append(result, export)
	}
	return result, nil
}

func (d *DB) ImportMemories(items []map[string]interface{}, projectDir string) (imported, skipped int, err error) {
	tx, err := d.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer d.UnlockAfterTx()

	now := time.Now().Format(time.RFC3339)
	for _, item := range items {
		id, _ := item["id"].(string)
		if id == "" {
			skipped++
			continue
		}
		// Check existing
		var exists string
		if tx.QueryRow("SELECT id FROM memories WHERE id=?", id).Scan(&exists) == nil {
			skipped++
			continue
		}
		if tx.QueryRow("SELECT id FROM user_memories WHERE id=?", id).Scan(&exists) == nil {
			skipped++
			continue
		}

		content, _ := item["content"].(string)
		scope, _ := item["scope"].(string)
		if scope == "" {
			scope = "project"
		}
		source, _ := item["source"].(string)
		if source == "" {
			source = "manual"
		}
		sessionID := 0
		if sid, ok := item["session_id"].(float64); ok {
			sessionID = int(sid)
		}
		createdAt, _ := item["created_at"].(string)
		if createdAt == "" {
			createdAt = now
		}

		tagsRaw := item["tags"]
		var tagsJSON string
		switch v := tagsRaw.(type) {
		case string:
			tagsJSON = v
		case []interface{}:
			b, _ := json.Marshal(v)
			tagsJSON = string(b)
		default:
			tagsJSON = "[]"
		}

		embeddingJSON := "[]"
		if emb, ok := item["embedding"]; ok && emb != nil {
			b, _ := json.Marshal(emb)
			embeddingJSON = string(b)
		}

		if scope == "user" {
			tx.Exec("INSERT INTO user_memories (id,content,tags,source,session_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
				id, content, tagsJSON, source, sessionID, createdAt, now)
			tx.Exec("INSERT INTO vec_user_memories (id,embedding) VALUES (?,?)", id, embeddingJSON)
		} else {
			pd, _ := item["project_dir"].(string)
			if pd == "" {
				pd = projectDir
			}
			tx.Exec("INSERT INTO memories (id,content,tags,scope,source,project_dir,session_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
				id, content, tagsJSON, scope, source, pd, sessionID, createdAt, now)
			tx.Exec("INSERT INTO vec_memories (id,embedding) VALUES (?,?)", id, embeddingJSON)
		}
		imported++
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return imported, skipped, nil
}

// SearchMemories performs vector similarity search (requires embedding from caller)
func (d *DB) SearchMemories(embedding []float32, scope, projectDir string, tags []string, topK int) ([]Memory, error) {
	if topK <= 0 {
		topK = 20
	}

	embJSON, _ := json.Marshal(embedding)

	var results []Memory

	if scope != "user" {
		sql := "SELECT m.*, v.distance FROM memories m JOIN (SELECT id, distance FROM vec_memories WHERE embedding MATCH ? ORDER BY distance LIMIT ?) v ON m.id=v.id"
		args := []interface{}{string(embJSON), topK * 2}
		if scope == "project" && projectDir != "" {
			sql += " AND m.project_dir=?"
			args = append(args, projectDir)
		}
		rows, err := d.Query(sql, args...)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				m, dist := scanMemoryWithDistance(rows)
				m.Similarity = 1 - (dist*dist)/2
				results = append(results, m)
			}
		}
	}

	if scope != "project" {
		sql := "SELECT um.id, um.content, um.tags, 'user' as scope, um.source, '' as project_dir, um.session_id, um.created_at, um.updated_at, v.distance FROM user_memories um JOIN (SELECT id, distance FROM vec_user_memories WHERE embedding MATCH ? ORDER BY distance LIMIT ?) v ON um.id=v.id"
		rows, err := d.Query(sql, string(embJSON), topK*2)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				m, dist := scanMemoryWithDistance(rows)
				m.Similarity = 1 - (dist*dist)/2
				results = append(results, m)
			}
		}
	}

	// Sort by similarity desc, take topK
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Similarity > results[i].Similarity {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
	if len(results) > topK {
		results = results[:topK]
	}

	// Round similarity
	for i := range results {
		results[i].Similarity = float64(int(results[i].Similarity*10000)) / 10000
	}

	return results, nil
}

// helpers

func (d *DB) queryMemories(query string, args ...interface{}) []Memory {
	rows, err := d.Query(query, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	var memories []Memory
	for rows.Next() {
		m := scanMemory(rows, cols)
		memories = append(memories, m)
	}
	return memories
}

func scanMemory(rows interface{ Scan(...interface{}) error }, cols []string) Memory {
	m := Memory{}
	var tagsJSON string

	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	rows.Scan(ptrs...)

	for i, col := range cols {
		v := vals[i]
		s := toString(v)
		switch col {
		case "id":
			m.ID = s
		case "content":
			m.Content = s
		case "tags":
			tagsJSON = s
		case "scope":
			m.Scope = s
		case "source":
			m.Source = s
		case "project_dir":
			m.ProjectDir = s
		case "session_id":
			if n, ok := v.(int64); ok {
				m.SessionID = int(n)
			}
		case "created_at":
			m.CreatedAt = s
		case "updated_at":
			m.UpdatedAt = s
		}
	}

	json.Unmarshal([]byte(tagsJSON), &m.Tags)
	if m.Tags == nil {
		m.Tags = []string{}
	}
	return m
}

func scanMemoryWithDistance(rows interface{ Scan(...interface{}) error }) (Memory, float64) {
	var id, content, tagsJSON, scope, source, projectDir, createdAt, updatedAt string
	var sessionID int64
	var distance float64
	rows.Scan(&id, &content, &tagsJSON, &scope, &source, &projectDir, &sessionID, &createdAt, &updatedAt, &distance)

	m := Memory{
		ID: id, Content: content, Scope: scope, Source: source,
		ProjectDir: projectDir, SessionID: int(sessionID),
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	json.Unmarshal([]byte(tagsJSON), &m.Tags)
	if m.Tags == nil {
		m.Tags = []string{}
	}
	return m, distance
}

func (d *DB) countTable(table, where string) int {
	var count int
	if where != "" {
		d.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, where)).Scan(&count)
	} else {
		d.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	}
	return count
}

func (d *DB) countTableWhere(table, where string, args ...interface{}) int {
	var count int
	d.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, where), args...).Scan(&count)
	return count
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func float32FromBits(b uint32) float32 {
	return math.Float32frombits(b)
}
