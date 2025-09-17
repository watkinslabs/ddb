package config

import (
	"ddb/pkg/types"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Manager implements the ConfigManager interface
type Manager struct {
	configDir string
}

// NewManager creates a new configuration manager
func NewManager(configDir string) *Manager {
	return &Manager{
		configDir: configDir,
	}
}

// LoadConfig loads a table configuration from a file
func (m *Manager) LoadConfig(configPath string) (*types.TableConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config types.TableConfig
	ext := strings.ToLower(filepath.Ext(configPath))

	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config file format: %s", ext)
	}

	return &config, nil
}

// SaveConfig saves a table configuration to a file
func (m *Manager) SaveConfig(config *types.TableConfig, configPath string) error {
	ext := strings.ToLower(filepath.Ext(configPath))
	var data []byte
	var err error

	switch ext {
	case ".yaml", ".yml":
		data, err = yaml.Marshal(config)
		if err != nil {
			return fmt.Errorf("failed to marshal config to YAML: %w", err)
		}
	case ".json":
		data, err = json.MarshalIndent(config, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal config to JSON: %w", err)
		}
	default:
		return fmt.Errorf("unsupported config file format: %s", ext)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// ListConfigs returns all available table configurations
func (m *Manager) ListConfigs() ([]*types.TableConfig, error) {
	if m.configDir == "" {
		return []*types.TableConfig{}, nil
	}

	var configs []*types.TableConfig

	err := filepath.Walk(m.configDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".yaml" || ext == ".yml" || ext == ".json" {
			config, err := m.LoadConfig(path)
			if err != nil {
				return fmt.Errorf("failed to load config %s: %w", path, err)
			}
			configs = append(configs, config)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list configs: %w", err)
	}

	return configs, nil
}

// ParseSQLConfig parses SQL CREATE TABLE statement into TableConfig
func ParseSQLConfig(sql string) (*types.TableConfig, error) {
	// This is a simplified parser for CREATE TABLE statements
	// In a full implementation, you'd use a proper SQL parser
	
	sql = strings.TrimSpace(sql)
	if !strings.HasPrefix(strings.ToUpper(sql), "CREATE TABLE") {
		return nil, fmt.Errorf("expected CREATE TABLE statement")
	}

	// Extract table name and columns (simplified parsing)
	// This would need a proper SQL parser in production
	config := &types.TableConfig{
		Format:    "csv",
		HasHeader: true,
		Delimiter: ",",
	}

	return config, nil
}