package settings

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

func SetAutoStart(enabled bool) error {
	switch runtime.GOOS {
	case "darwin":
		return setAutoStartMacOS(enabled)
	case "windows":
		return setAutoStartWindows(enabled)
	case "linux":
		return setAutoStartLinux(enabled)
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
}

// macOS: Launch Agent plist
func setAutoStartMacOS(enabled bool) error {
	home, _ := os.UserHomeDir()
	plistPath := filepath.Join(home, "Library", "LaunchAgents", "com.aivectormemory.desktop.plist")

	if !enabled {
		os.Remove(plistPath)
		return nil
	}

	exePath, err := os.Executable()
	if err != nil {
		return err
	}

	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.aivectormemory.desktop</string>
    <key>ProgramArguments</key>
    <array>
        <string>%s</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>`, exePath)

	os.MkdirAll(filepath.Dir(plistPath), 0755)
	return os.WriteFile(plistPath, []byte(plist), 0644)
}

// Windows: Registry Run key (stubbed - actual implementation needs golang.org/x/sys/windows/registry)
func setAutoStartWindows(enabled bool) error {
	// Windows implementation requires registry access
	// Will be implemented with golang.org/x/sys/windows/registry when building on Windows
	return fmt.Errorf("windows auto-start: not implemented in this build")
}

// Linux: XDG autostart desktop file
func setAutoStartLinux(enabled bool) error {
	home, _ := os.UserHomeDir()
	desktopFile := filepath.Join(home, ".config", "autostart", "aivectormemory.desktop")

	if !enabled {
		os.Remove(desktopFile)
		return nil
	}

	exePath, err := os.Executable()
	if err != nil {
		return err
	}

	content := fmt.Sprintf(`[Desktop Entry]
Type=Application
Name=AIVectorMemory
Exec=%s
Terminal=false
X-GNOME-Autostart-enabled=true
`, exePath)

	os.MkdirAll(filepath.Dir(desktopFile), 0755)
	return os.WriteFile(desktopFile, []byte(content), 0644)
}
