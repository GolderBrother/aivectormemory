package backup

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type BackupInfo struct {
	Filename  string `json:"filename"`
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	SizeMB    string `json:"size_mb"`
	CreatedAt string `json:"created_at"`
}

func BackupDB(dbPath, backupDir string) (*BackupInfo, error) {
	if backupDir == "" {
		backupDir = filepath.Join(filepath.Dir(dbPath), "backups")
	}
	os.MkdirAll(backupDir, 0755)

	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("memory_%s.db", timestamp)
	destPath := filepath.Join(backupDir, filename)

	src, err := os.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer src.Close()

	dst, err := os.Create(destPath)
	if err != nil {
		return nil, fmt.Errorf("create backup: %w", err)
	}
	defer dst.Close()

	written, err := io.Copy(dst, src)
	if err != nil {
		return nil, fmt.Errorf("copy: %w", err)
	}

	return &BackupInfo{
		Filename:  filename,
		Path:      destPath,
		SizeBytes: written,
		SizeMB:    fmt.Sprintf("%.2f", float64(written)/1024/1024),
		CreatedAt: time.Now().Format(time.RFC3339),
	}, nil
}

func RestoreDB(dbPath, backupPath string) error {
	// Auto-backup current before restore
	_, err := BackupDB(dbPath, "")
	if err != nil {
		return fmt.Errorf("auto-backup before restore: %w", err)
	}

	src, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("open backup: %w", err)
	}
	defer src.Close()

	dst, err := os.Create(dbPath)
	if err != nil {
		return fmt.Errorf("create restore target: %w", err)
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}

func ListBackups(dbPath string) ([]BackupInfo, error) {
	backupDir := filepath.Join(filepath.Dir(dbPath), "backups")
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []BackupInfo{}, nil
		}
		return nil, err
	}

	var backups []BackupInfo
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		backups = append(backups, BackupInfo{
			Filename:  e.Name(),
			Path:      filepath.Join(backupDir, e.Name()),
			SizeBytes: info.Size(),
			SizeMB:    fmt.Sprintf("%.2f", float64(info.Size())/1024/1024),
			CreatedAt: info.ModTime().Format(time.RFC3339),
		})
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].CreatedAt > backups[j].CreatedAt
	})

	return backups, nil
}
