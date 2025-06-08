package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration values
type Config struct {
	PriorityQMainQDiff           int64 `yaml:"priority_time"`
	ZombieWhenAllPatternNotMatch bool
	MaxZombifiedRetries          uint8 `yaml:"max_retries"`
	RetryAfterTimeout            int   `yaml:"retry_timeout"`
	ReadTimedOutAfterConnecting  bool  `yaml:"consume_expired"`
	CleanupTimeout               int64 `yaml:"cleanup_timeout"`
	Port                         int   `yaml:"port"`
}

func DefaultConfig() *Config {
	return &Config{
		PriorityQMainQDiff:           9000, // 9 seconds
		ZombieWhenAllPatternNotMatch: false,
		MaxZombifiedRetries:          2,
		RetryAfterTimeout:            10,
		ReadTimedOutAfterConnecting:  true,
		CleanupTimeout:               86400000, // 24 hours
		Port:                         8080,
	}
}

// LoadConfig loads configuration from YAML file with fallback to default values
func LoadConfig() (*Config, error) {
	config := DefaultConfig()

	configPath := "config.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return config, fmt.Errorf("failed to get user home directory: %v", err)
		}

		if runtime.GOOS == "windows" {
			configPath = filepath.Join(homeDir, "cue", "config.yaml")
		} else {
			configPath = filepath.Join(homeDir, ".config", "cue", "config.yaml")
		}

		configDir := filepath.Dir(configPath)
		if err := os.MkdirAll(configDir, 0755); err != nil {
			return config, fmt.Errorf("failed to create config directory: %v", err)
		}
	}

	if _, err := os.Stat(configPath); err == nil {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return config, fmt.Errorf("failed to read config file: %v", err)
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return config, fmt.Errorf("failed to parse config file: %v", err)
		}
	}
	return config, nil
}
