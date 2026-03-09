package db

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Project struct {
	ProjectDir   string `json:"project_dir"`
	Name         string `json:"name"`
	Memories     int    `json:"memories"`
	UserMemories int    `json:"user_memories"`
	Issues       int    `json:"issues"`
	Tags         int    `json:"tags"`
}

func (d *DB) GetProjects() ([]Project, error) {
	projects := map[string]*Project{}

	// Memory counts per project
	rows, err := d.Query("SELECT project_dir, COUNT(*) as cnt FROM memories GROUP BY project_dir")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pd string
		var cnt int
		if err := rows.Scan(&pd, &cnt); err != nil {
			return nil, err
		}
		if pd == "" {
			continue
		}
		projects[pd] = &Project{ProjectDir: pd, Memories: cnt}
	}

	// Issue counts (active + archive)
	for _, table := range []string{"issues", "issues_archive"} {
		rows, err := d.Query(fmt.Sprintf("SELECT project_dir, COUNT(*) as cnt FROM %s GROUP BY project_dir", table))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var pd string
			var cnt int
			if err := rows.Scan(&pd, &cnt); err != nil {
				rows.Close()
				return nil, err
			}
			if pd == "" {
				continue
			}
			if _, ok := projects[pd]; !ok {
				projects[pd] = &Project{ProjectDir: pd}
			}
			projects[pd].Issues += cnt
		}
		rows.Close()
	}

	// Session state projects
	rows, err = d.Query("SELECT project_dir FROM session_state")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pd string
		if err := rows.Scan(&pd); err != nil {
			return nil, err
		}
		if pd == "" {
			continue
		}
		if _, ok := projects[pd]; !ok {
			projects[pd] = &Project{ProjectDir: pd}
		}
	}

	// Tag counts per project
	tagRows, err := d.Query("SELECT project_dir, tags FROM memories")
	if err != nil {
		return nil, err
	}
	defer tagRows.Close()
	projTags := map[string]map[string]bool{}
	for tagRows.Next() {
		var pd, tagsJSON string
		if err := tagRows.Scan(&pd, &tagsJSON); err != nil {
			return nil, err
		}
		if pd == "" {
			continue
		}
		if _, ok := projTags[pd]; !ok {
			projTags[pd] = map[string]bool{}
		}
		var tags []string
		json.Unmarshal([]byte(tagsJSON), &tags)
		for _, t := range tags {
			projTags[pd][t] = true
		}
	}

	// User memory count & tags
	var userCount int
	d.QueryRow("SELECT COUNT(*) FROM user_memories").Scan(&userCount)

	userTags := map[string]bool{}
	uTagRows, err := d.Query("SELECT tags FROM user_memories")
	if err == nil {
		defer uTagRows.Close()
		for uTagRows.Next() {
			var tagsJSON string
			if err := uTagRows.Scan(&tagsJSON); err == nil {
				var tags []string
				json.Unmarshal([]byte(tagsJSON), &tags)
				for _, t := range tags {
					userTags[t] = true
				}
			}
		}
	}

	result := make([]Project, 0, len(projects))
	for pd, p := range projects {
		merged := map[string]bool{}
		for t := range projTags[pd] {
			merged[t] = true
		}
		for t := range userTags {
			merged[t] = true
		}
		parts := strings.Replace(pd, "\\", "/", -1)
		idx := strings.LastIndex(parts, "/")
		name := parts
		if idx >= 0 {
			name = parts[idx+1:]
		}
		p.Name = name
		p.UserMemories = userCount
		p.Tags = len(merged)
		result = append(result, *p)
	}
	return result, nil
}

func (d *DB) AddProject(projectDir string) error {
	projectDir = strings.TrimSpace(strings.Replace(projectDir, "\\", "/", -1))
	if projectDir == "" {
		return fmt.Errorf("project_dir is required")
	}
	now := time.Now().Format(time.RFC3339)
	_, err := d.Exec(
		"INSERT OR IGNORE INTO session_state (project_dir, is_blocked, block_reason, next_step, current_task, progress, recent_changes, pending, updated_at) VALUES (?,0,'','','','[]','[]','[]',?)",
		projectDir, now)
	return err
}

func (d *DB) DeleteProject(projectDir string) (int, error) {
	if projectDir == "" {
		return 0, fmt.Errorf("cannot delete empty project")
	}

	tx, err := d.Begin()
	if err != nil {
		return 0, err
	}
	defer d.UnlockAfterTx()

	// Get memory IDs
	rows, err := tx.Query("SELECT id FROM memories WHERE project_dir = ?", projectDir)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	var memIDs []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		memIDs = append(memIDs, id)
	}
	rows.Close()

	if len(memIDs) > 0 {
		ph := strings.Repeat("?,", len(memIDs))
		ph = ph[:len(ph)-1]
		args := make([]interface{}, len(memIDs))
		for i, id := range memIDs {
			args[i] = id
		}
		tx.Exec(fmt.Sprintf("DELETE FROM vec_memories WHERE id IN (%s)", ph), args...)
		tx.Exec(fmt.Sprintf("DELETE FROM memory_tags WHERE memory_id IN (%s)", ph), args...)
		tx.Exec(fmt.Sprintf("DELETE FROM memories WHERE id IN (%s)", ph), args...)
	}

	// Delete tasks
	tx.Exec("DELETE FROM tasks WHERE project_dir = ?", projectDir)
	tx.Exec("DELETE FROM tasks_archive WHERE project_dir = ?", projectDir)
	tx.Exec("DELETE FROM issues WHERE project_dir = ?", projectDir)
	tx.Exec("DELETE FROM issues_archive WHERE project_dir = ?", projectDir)
	tx.Exec("DELETE FROM session_state WHERE project_dir = ?", projectDir)

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(memIDs), nil
}
