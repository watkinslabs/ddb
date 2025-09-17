package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "ddb",
	Short: "A stateless SQL engine for querying text files",
	Long: `DDB is a high-performance, stateless SQL engine that allows you to run
MySQL-style queries against text files (CSV, JSON, YAML) without loading
everything into memory. It supports parallel processing and streaming evaluation.`,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(queryCmd)
	rootCmd.AddCommand(configCmd)
	rootCmd.AddCommand(versionCmd)
}