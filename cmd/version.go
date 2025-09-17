package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var (
	version   = "1.0.0"
	gitCommit = "dev"
	buildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Long:  "Print version information for ddb",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("ddb version %s\n", version)
		fmt.Printf("Git commit: %s\n", gitCommit)
		fmt.Printf("Build date: %s\n", buildDate)
		fmt.Printf("Go version: %s\n", runtime.Version())
		fmt.Printf("OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	},
}