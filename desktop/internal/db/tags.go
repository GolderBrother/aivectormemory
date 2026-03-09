package db

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type TagInfo struct {
	Name         string `json:"name"`
	Count        int    `json:"count"`
	ProjectCount int    `json:"project_count"`
	UserCount    int    `json:"user_count"`
}

func (d *DB) GetTags(projectDir, query string) ([]TagInfo, error) {
	// Project tag counts
	projCounts := map[string]int{}
	rows, err := d.Query(
		"SELECT mt.tag, COUNT(*) as cnt FROM memory_tags mt JOIN memories m ON mt.memory_id=m.id WHERE m.project_dir=? GROUP BY mt.tag",
		projectDir)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var tag string
			var cnt int
			rows.Scan(&tag, &cnt)
			projCounts[tag] = cnt
		}
	}

	// User tag counts
	userCounts := map[string]int{}
	rows2, err := d.Query("SELECT tag, COUNT(*) as cnt FROM user_memory_tags GROUP BY tag")
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var tag string
			var cnt int
			rows2.Scan(&tag, &cnt)
			userCounts[tag] = cnt
		}
	}

	// Merge
	allTags := map[string]bool{}
	for t := range projCounts {
		allTags[t] = true
	}
	for t := range userCounts {
		allTags[t] = true
	}

	result := make([]TagInfo, 0, len(allTags))
	for t := range allTags {
		pc := projCounts[t]
		uc := userCounts[t]
		info := TagInfo{Name: t, Count: pc + uc, ProjectCount: pc, UserCount: uc}
		if query != "" && !strings.Contains(strings.ToLower(t), strings.ToLower(query)) {
			continue
		}
		result = append(result, info)
	}

	// Sort by count desc
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result, nil
}

func (d *DB) RenameTag(projectDir, oldName, newName string) (int, error) {
	if oldName == "" || newName == "" {
		return 0, fmt.Errorf("old_name and new_name required")
	}
	now := time.Now().Format(time.RFC3339)
	updated := 0

	ids := d.getMemoryIDsWithTag(projectDir, oldName)
	for _, mid := range ids {
		table, tags := d.getMemoryTableAndTags(mid)
		if table == "" {
			continue
		}
		newTags := make([]string, 0, len(tags))
		seen := map[string]bool{}
		for _, t := range tags {
			name := t
			if t == oldName {
				name = newName
			}
			if !seen[name] {
				newTags = append(newTags, name)
				seen[name] = true
			}
		}
		tagsJSON, _ := json.Marshal(newTags)
		d.Exec(fmt.Sprintf("UPDATE %s SET tags=?, updated_at=? WHERE id=?", table), string(tagsJSON), now, mid)
		updated++
	}
	return updated, nil
}

func (d *DB) MergeTags(projectDir string, sourceTags []string, targetName string) (int, error) {
	if len(sourceTags) == 0 || targetName == "" {
		return 0, fmt.Errorf("source_tags and target_name required")
	}
	now := time.Now().Format(time.RFC3339)
	sourceSet := map[string]bool{}
	for _, s := range sourceTags {
		sourceSet[s] = true
	}

	seen := map[string]bool{}
	updated := 0

	for _, src := range sourceTags {
		ids := d.getMemoryIDsWithTag(projectDir, src)
		for _, mid := range ids {
			if seen[mid] {
				continue
			}
			seen[mid] = true
			table, tags := d.getMemoryTableAndTags(mid)
			if table == "" {
				continue
			}
			newTags := make([]string, 0, len(tags))
			dedupSet := map[string]bool{}
			for _, t := range tags {
				name := t
				if sourceSet[t] {
					name = targetName
				}
				if !dedupSet[name] {
					newTags = append(newTags, name)
					dedupSet[name] = true
				}
			}
			tagsJSON, _ := json.Marshal(newTags)
			d.Exec(fmt.Sprintf("UPDATE %s SET tags=?, updated_at=? WHERE id=?", table), string(tagsJSON), now, mid)
			updated++
		}
	}
	return updated, nil
}

func (d *DB) DeleteTags(projectDir string, tagNames []string) (int, error) {
	if len(tagNames) == 0 {
		return 0, fmt.Errorf("tags required")
	}
	now := time.Now().Format(time.RFC3339)
	deleteSet := map[string]bool{}
	for _, t := range tagNames {
		deleteSet[t] = true
	}

	seen := map[string]bool{}
	updated := 0

	for _, tn := range tagNames {
		ids := d.getMemoryIDsWithTag(projectDir, tn)
		for _, mid := range ids {
			if seen[mid] {
				continue
			}
			seen[mid] = true
			table, tags := d.getMemoryTableAndTags(mid)
			if table == "" {
				continue
			}
			newTags := make([]string, 0)
			for _, t := range tags {
				if !deleteSet[t] {
					newTags = append(newTags, t)
				}
			}
			if len(newTags) != len(tags) {
				tagsJSON, _ := json.Marshal(newTags)
				d.Exec(fmt.Sprintf("UPDATE %s SET tags=?, updated_at=? WHERE id=?", table), string(tagsJSON), now, mid)
				updated++
			}
		}
	}
	return updated, nil
}

// helpers

func (d *DB) getMemoryIDsWithTag(projectDir, tag string) []string {
	var ids []string

	// Project memories
	rows, err := d.Query(
		"SELECT m.id FROM memories m JOIN memory_tags mt ON m.id=mt.memory_id WHERE mt.tag=? AND m.project_dir=?",
		tag, projectDir)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id string
			rows.Scan(&id)
			ids = append(ids, id)
		}
	}

	// User memories
	rows2, err := d.Query(
		"SELECT um.id FROM user_memories um JOIN user_memory_tags umt ON um.id=umt.memory_id WHERE umt.tag=?", tag)
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var id string
			rows2.Scan(&id)
			ids = append(ids, id)
		}
	}

	return ids
}

func (d *DB) getMemoryTableAndTags(id string) (string, []string) {
	// Check memories table
	var tagsJSON string
	err := d.QueryRow("SELECT tags FROM memories WHERE id=?", id).Scan(&tagsJSON)
	if err == nil {
		var tags []string
		json.Unmarshal([]byte(tagsJSON), &tags)
		return "memories", tags
	}
	// Check user_memories
	err = d.QueryRow("SELECT tags FROM user_memories WHERE id=?", id).Scan(&tagsJSON)
	if err == nil {
		var tags []string
		json.Unmarshal([]byte(tagsJSON), &tags)
		return "user_memories", tags
	}
	return "", nil
}
