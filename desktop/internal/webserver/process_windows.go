//go:build windows

package webserver

import (
	"os/exec"
)

func setProcAttr(cmd *exec.Cmd) {
	// Windows doesn't support Setpgid; no special process attributes needed
}

func killProcess(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		cmd.Process.Kill()
	}
}
