package main

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

func RunCmd() {
	var rootCmd = &cobra.Command{
		Use:   "sceduler",
		Short: "scheduler cli",
	}

	var serverCmd = &cobra.Command{
		Use:   "server",
		Short: "Runs the scheduler server",
		Run: func(cmd *cobra.Command, args []string) {
			s := NewServer()
			log.Fatal(s.Start())
		},
	}

	rootCmd.AddCommand(serverCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
	}
}
