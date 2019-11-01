package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

const version = "0.1.0"

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use: "version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("mqtt-loadtest/%v %v\n", version, runtime.Version())

	},
}
