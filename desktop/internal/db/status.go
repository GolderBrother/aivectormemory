package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type SessionState struct {
	ID            int      `json:"id"`
	ProjectDir    string   `json:"project_dir"`
	IsBlocked     bool     `json:"is_blocked"`
	BlockReason   string   `json:"block_reason"`
	NextStep      string   `json:"next_step"`
	CurrentTask   string   `json:"current_task"`
	Progress      []string `json:"progress"`
	RecentChanges []string `json:"recent_changes"`
	Pending       []string `json:"pending"`
	UpdatedAt     string   `json:"updated_at"`
}

func (d *DB) GetStatus(projectDir string) (*SessionState, error) {
	var s SessionState
	var isBlocked int
	var progressJSON, changesJSON, pendingJSON string

	err := d.QueryRow(
		"SELECT id, project_dir, is_blocked, block_reason, next_step, current_task, progress, recent_changes, pending, updated_at FROM session_state WHERE project_dir=?",
		projectDir).Scan(
		&s.ID, &s.ProjectDir, &isBlocked, &s.BlockReason, &s.NextStep, &s.CurrentTask,
		&progressJSON, &changesJSON, &pendingJSON, &s.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("session state not found for project: %s", projectDir)
	}

	s.IsBlocked = isBlocked != 0
	json.Unmarshal([]byte(progressJSON), &s.Progress)
	json.Unmarshal([]byte(changesJSON), &s.RecentChanges)
	json.Unmarshal([]byte(pendingJSON), &s.Pending)

	if s.Progress == nil {
		s.Progress = []string{}
	}
	if s.RecentChanges == nil {
		s.RecentChanges = []string{}
	}
	if s.Pending == nil {
		s.Pending = []string{}
	}

	return &s, nil
}

func (d *DB) UpdateStatus(projectDir string, fields map[string]interface{}, clearFields []string) (*SessionState, error) {
	now := time.Now().Format(time.RFC3339)

	sets := []string{"updated_at=?"}
	args := []interface{}{now}

	for k, v := range fields {
		switch k {
		case "is_blocked":
			blocked := 0
			if b, ok := v.(bool); ok && b {
				blocked = 1
			}
			sets = append(sets, "is_blocked=?")
			args = append(args, blocked)
		case "block_reason", "next_step", "current_task":
			sets = append(sets, k+"=?")
			args = append(args, fmt.Sprintf("%v", v))
		case "progress", "recent_changes", "pending":
			b, _ := json.Marshal(v)
			sets = append(sets, k+"=?")
			args = append(args, string(b))
		}
	}

	for _, cf := range clearFields {
		switch cf {
		case "recent_changes", "pending":
			sets = append(sets, cf+"=?")
			args = append(args, "[]")
		}
	}

	args = append(args, projectDir)
	_, err := d.Exec(
		fmt.Sprintf("UPDATE session_state SET %s WHERE project_dir=?", joinStrings(sets, ",")),
		args...)
	if err != nil {
		return nil, err
	}

	return d.GetStatus(projectDir)
}

func joinStrings(s []string, sep string) string {
	result := ""
	for i, v := range s {
		if i > 0 {
			result += sep
		}
		result += v
	}
	return result
}
