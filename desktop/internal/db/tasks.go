package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type Task struct {
	ID         int     `json:"id"`
	ProjectDir string  `json:"project_dir"`
	FeatureID  string  `json:"feature_id"`
	Title      string  `json:"title"`
	Status     string  `json:"status"`
	SortOrder  int     `json:"sort_order"`
	ParentID   int     `json:"parent_id"`
	TaskType   string  `json:"task_type"`
	Metadata   string  `json:"metadata"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  string  `json:"updated_at"`
	Children   []*Task `json:"children,omitempty"`
}

type TaskGroup struct {
	FeatureID string `json:"feature_id"`
	Tasks     []Task `json:"tasks"`
	Total     int    `json:"total"`
	Done      int    `json:"done"`
}

func (d *DB) GetTasks(projectDir, featureID, status, keyword string) ([]TaskGroup, error) {
	where := []string{"project_dir=?"}
	args := []interface{}{projectDir}

	if featureID != "" {
		where = append(where, "feature_id=?")
		args = append(args, featureID)
	}
	if status != "" {
		where = append(where, "status=?")
		args = append(args, status)
	}
	if keyword != "" {
		where = append(where, "title LIKE ?")
		args = append(args, "%"+keyword+"%")
	}

	whereClause := strings.Join(where, " AND ")
	rows, err := d.Query(fmt.Sprintf("SELECT * FROM tasks WHERE %s ORDER BY feature_id, sort_order, id", whereClause), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allTasks := scanTasks(rows)
	return buildTaskGroups(allTasks, projectDir, d), nil
}

func (d *DB) GetArchivedTasks(projectDir, featureID string) ([]TaskGroup, error) {
	where := []string{"project_dir=?"}
	args := []interface{}{projectDir}
	if featureID != "" {
		where = append(where, "feature_id=?")
		args = append(args, featureID)
	}
	whereClause := strings.Join(where, " AND ")
	rows, err := d.Query(fmt.Sprintf("SELECT id, project_dir, feature_id, title, status, sort_order, parent_id, task_type, metadata, created_at, updated_at FROM tasks_archive WHERE %s ORDER BY feature_id, sort_order, id", whereClause), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allTasks := scanTasks(rows)
	return buildTaskGroups(allTasks, projectDir, d), nil
}

func (d *DB) CreateTasks(projectDir, featureID string, tasks []map[string]interface{}, taskType string) (int, error) {
	if featureID == "" {
		return 0, fmt.Errorf("feature_id is required")
	}
	if taskType == "" {
		taskType = "manual"
	}
	now := time.Now().Format(time.RFC3339)
	created := 0

	tx, err := d.Begin()
	if err != nil {
		return 0, err
	}
	defer d.UnlockAfterTx()

	for i, t := range tasks {
		title, _ := t["title"].(string)
		if title == "" {
			continue
		}
		sortOrder := i + 1
		if so, ok := t["sort_order"].(float64); ok {
			sortOrder = int(so)
		}

		result, err := tx.Exec(
			"INSERT INTO tasks (project_dir, feature_id, title, status, sort_order, parent_id, task_type, metadata, created_at, updated_at) VALUES (?,?,?,'pending',?,0,?,'{}',?,?)",
			projectDir, featureID, title, sortOrder, taskType, now, now)
		if err != nil {
			tx.Rollback()
			return 0, err
		}
		parentID, _ := result.LastInsertId()
		created++

		// Children
		if children, ok := t["children"].([]interface{}); ok {
			for j, child := range children {
				cm, ok := child.(map[string]interface{})
				if !ok {
					continue
				}
				childTitle, _ := cm["title"].(string)
				if childTitle == "" {
					continue
				}
				childSort := j + 1
				if so, ok := cm["sort_order"].(float64); ok {
					childSort = int(so)
				}
				_, err := tx.Exec(
					"INSERT INTO tasks (project_dir, feature_id, title, status, sort_order, parent_id, task_type, metadata, created_at, updated_at) VALUES (?,?,?,'pending',?,?,?,'{}',?,?)",
					projectDir, featureID, childTitle, childSort, int(parentID), taskType, now, now)
				if err != nil {
					tx.Rollback()
					return 0, err
				}
				created++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return created, nil
}

func (d *DB) UpdateTask(id int, projectDir string, fields map[string]interface{}) (*Task, error) {
	sets := []string{}
	args := []interface{}{}

	for k, v := range fields {
		switch k {
		case "status", "title":
			sets = append(sets, k+"=?")
			args = append(args, v)
		}
	}

	if len(sets) > 0 {
		sets = append(sets, "updated_at=?")
		args = append(args, time.Now().Format(time.RFC3339))
		args = append(args, id, projectDir)
		_, err := d.Exec(fmt.Sprintf("UPDATE tasks SET %s WHERE id=? AND project_dir=?", strings.Join(sets, ",")), args...)
		if err != nil {
			return nil, err
		}
	}

	row, err := d.Query("SELECT * FROM tasks WHERE id=? AND project_dir=?", id, projectDir)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	tasks := scanTasks(row)
	if len(tasks) == 0 {
		return nil, fmt.Errorf("task not found: %d", id)
	}
	return &tasks[0], nil
}

func (d *DB) DeleteTask(id int, projectDir string) error {
	// Delete children first
	d.Exec("DELETE FROM tasks WHERE parent_id=? AND project_dir=?", id, projectDir)
	res, err := d.Exec("DELETE FROM tasks WHERE id=? AND project_dir=?", id, projectDir)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("task not found: %d", id)
	}
	return nil
}

func (d *DB) DeleteTasksByFeature(featureID, projectDir string) (int, error) {
	res, err := d.Exec("DELETE FROM tasks WHERE feature_id=? AND project_dir=?", featureID, projectDir)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// helpers

func buildTaskGroups(allTasks []Task, projectDir string, d *DB) []TaskGroup {
	// Build parent-child tree
	taskMap := map[int]*Task{}
	for i := range allTasks {
		allTasks[i].Children = []*Task{}
		taskMap[allTasks[i].ID] = &allTasks[i]
	}

	var roots []Task
	for i := range allTasks {
		if allTasks[i].ParentID > 0 {
			if parent, ok := taskMap[allTasks[i].ParentID]; ok {
				parent.Children = append(parent.Children, &allTasks[i])
				continue
			}
		}
		roots = append(roots, allTasks[i])
	}

	// Group by feature_id
	groupMap := map[string]*TaskGroup{}
	var order []string
	for _, t := range roots {
		fid := t.FeatureID
		if _, ok := groupMap[fid]; !ok {
			groupMap[fid] = &TaskGroup{FeatureID: fid}
			order = append(order, fid)
		}
		groupMap[fid].Tasks = append(groupMap[fid].Tasks, t)
	}

	// Count progress
	for fid, g := range groupMap {
		var total, done int
		d.QueryRow("SELECT COUNT(*) FROM tasks WHERE project_dir=? AND feature_id=?", projectDir, fid).Scan(&total)
		d.QueryRow("SELECT COUNT(*) FROM tasks WHERE project_dir=? AND feature_id=? AND status='completed'", projectDir, fid).Scan(&done)
		g.Total = total
		g.Done = done
	}

	result := make([]TaskGroup, 0, len(order))
	for _, fid := range order {
		result = append(result, *groupMap[fid])
	}
	return result
}

func scanTasks(rows *sql.Rows) []Task {
	var tasks []Task
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)

		t := Task{}
		for i, col := range cols {
			s := toString(vals[i])
			switch col {
			case "id":
				if n, ok := vals[i].(int64); ok {
					t.ID = int(n)
				}
			case "project_dir":
				t.ProjectDir = s
			case "feature_id":
				t.FeatureID = s
			case "title":
				t.Title = s
			case "status":
				t.Status = s
			case "sort_order":
				if n, ok := vals[i].(int64); ok {
					t.SortOrder = int(n)
				}
			case "parent_id":
				if n, ok := vals[i].(int64); ok {
					t.ParentID = int(n)
				}
			case "task_type":
				t.TaskType = s
			case "metadata":
				t.Metadata = s
			case "created_at":
				t.CreatedAt = s
			case "updated_at":
				t.UpdatedAt = s
			}
		}
		tasks = append(tasks, t)
	}
	if tasks == nil {
		tasks = []Task{}
	}
	return tasks
}
