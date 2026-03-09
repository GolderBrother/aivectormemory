package embedding

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Engine struct {
	PythonPath string
}

func NewEngine(pythonPath string) *Engine {
	if pythonPath == "" {
		pythonPath = DetectPython()
	}
	return &Engine{PythonPath: pythonPath}
}

func (e *Engine) Encode(text string) ([]float32, error) {
	if e.PythonPath == "" {
		return nil, fmt.Errorf("python not found")
	}

	// Write input to temp file
	tmpDir := os.TempDir()
	inputFile := filepath.Join(tmpDir, "avm_embed_input.json")
	outputFile := filepath.Join(tmpDir, "avm_embed_output.json")

	inputData, _ := json.Marshal(map[string]string{"text": text})
	if err := os.WriteFile(inputFile, inputData, 0644); err != nil {
		return nil, fmt.Errorf("write input: %w", err)
	}
	defer os.Remove(inputFile)
	defer os.Remove(outputFile)

	script := fmt.Sprintf(`
import json, sys
try:
    from aivectormemory.embedding.engine import EmbeddingEngine
    with open(%q) as f:
        data = json.load(f)
    engine = EmbeddingEngine()
    embedding = engine.encode(data["text"])
    with open(%q, "w") as f:
        json.dump({"embedding": embedding}, f)
except Exception as e:
    with open(%q, "w") as f:
        json.dump({"error": str(e)}, f)
`, inputFile, outputFile, outputFile)

	scriptFile := filepath.Join(tmpDir, "avm_embed_script.py")
	if err := os.WriteFile(scriptFile, []byte(script), 0644); err != nil {
		return nil, fmt.Errorf("write script: %w", err)
	}
	defer os.Remove(scriptFile)

	cmd := exec.Command(e.PythonPath, scriptFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("python exec: %w, output: %s", err, string(output))
	}

	resultData, err := os.ReadFile(outputFile)
	if err != nil {
		return nil, fmt.Errorf("read output: %w", err)
	}

	var result struct {
		Embedding []float32 `json:"embedding"`
		Error     string    `json:"error"`
	}
	if err := json.Unmarshal(resultData, &result); err != nil {
		return nil, fmt.Errorf("parse output: %w", err)
	}
	if result.Error != "" {
		return nil, fmt.Errorf("embedding error: %s", result.Error)
	}

	return result.Embedding, nil
}

func (e *Engine) EncodeBatch(texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	for i, text := range texts {
		emb, err := e.Encode(text)
		if err != nil {
			return nil, fmt.Errorf("encode text %d: %w", i, err)
		}
		results[i] = emb
	}
	return results, nil
}

// DetectPython finds a Python interpreter with aivectormemory installed
func DetectPython() string {
	candidates := []string{}

	// Check project venv first
	home, _ := os.UserHomeDir()
	venvPaths := []string{
		filepath.Join(home, "item", "run-memory-mcp-server", ".venv", "bin", "python3"),
		filepath.Join(home, "item", "run-memory-mcp-server", ".venv", "bin", "python"),
	}
	candidates = append(candidates, venvPaths...)

	// System paths
	systemPaths := []string{"python3", "python"}
	candidates = append(candidates, systemPaths...)

	// Common installation paths
	commonPaths := []string{
		"/usr/local/bin/python3",
		"/usr/bin/python3",
		"/opt/homebrew/bin/python3",
	}
	candidates = append(candidates, commonPaths...)

	for _, py := range candidates {
		path := py
		if !filepath.IsAbs(path) {
			found, err := exec.LookPath(path)
			if err != nil {
				continue
			}
			path = found
		}
		if _, err := os.Stat(path); err != nil {
			continue
		}
		// Verify aivectormemory is importable
		out, err := exec.Command(path, "-c", "import aivectormemory; print('ok')").Output()
		if err == nil && strings.TrimSpace(string(out)) == "ok" {
			return path
		}
	}
	return ""
}
