package db

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func (d *DB) LoadVecExtension() error {
	vecPath, err := findVecExtension()
	if err != nil {
		return fmt.Errorf("sqlite-vec not found: %w", err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	_, err = d.conn.Exec("SELECT load_extension(?)", vecPath)
	if err != nil {
		return fmt.Errorf("load sqlite-vec extension: %w", err)
	}
	return nil
}

func findVecExtension() (string, error) {
	candidates := []string{}

	// 1. Try Python package path
	if path := findVecViaPython(); path != "" {
		candidates = append(candidates, path)
	}

	// 2. Common locations
	home, _ := os.UserHomeDir()
	ext := ".so"
	if runtime.GOOS == "darwin" {
		ext = ".dylib"
	} else if runtime.GOOS == "windows" {
		ext = ".dll"
	}

	candidates = append(candidates,
		filepath.Join(home, ".aivectormemory", "vec0"+ext),
		filepath.Join(home, ".aivectormemory", "vec0"),
	)

	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
		// vec0 without extension (SQLite auto-appends)
		noExt := strings.TrimSuffix(c, ext)
		if noExt != c {
			if _, err := os.Stat(noExt); err == nil {
				return noExt, nil
			}
		}
	}

	return "", fmt.Errorf("vec0 extension not found in any known location")
}

func findVecViaPython() string {
	pythonPaths := []string{"python3", "python"}
	for _, py := range pythonPaths {
		out, err := exec.Command(py, "-c", "import sqlite_vec; print(sqlite_vec.loadable_path())").Output()
		if err == nil {
			path := strings.TrimSpace(string(out))
			if path != "" {
				return path
			}
		}
	}
	return ""
}
