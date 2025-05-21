package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ayushmd/delayedQ/pkg"
	"github.com/spf13/cobra"
)

func RunCmd() {
	var addr string
	serverAddr := fmt.Sprintf("%s:%d", addr, Port)
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

	var listQueueCmd = &cobra.Command{
		Use:   "list",
		Short: "List all queues",
		Run: func(cmd *cobra.Command, args []string) {
			cli, err := pkg.NewSchedulerClient(serverAddr)
			if err != nil {
				fmt.Println("Failed to connect server")
			}
			defer cli.Close()
			qs, err := cli.ListQueues()
			if err != nil {
				fmt.Println("Failed to list queues")
			}
			// qs, _ := ListAllQueues()
			for _, q := range qs {
				fmt.Println(q)
			}
		},
	}

	var deleteQueueCmd = &cobra.Command{
		Use:   "delete [queue_name]",
		Short: "Delete a queue by name",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			queueName := args[0]
			fmt.Printf("Are you sure, message will not be recieved from '%s' if deleted? (y/N): ", queueName)
			var response string
			fmt.Scanln(&response)
			if response != "y" && response != "Y" {
				return
			}
			cli, err := pkg.NewSchedulerClient(serverAddr)
			if err != nil {
				fmt.Println("Failed to connect server")
			}
			defer cli.Close()
			err = cli.DeleteQueue(queueName)
			if err != nil {
				fmt.Println("Failed to delete queues")
			}
		},
	}

	var createQueueCmd = &cobra.Command{
		Use:   "create [queue_name]",
		Short: "Delete a queue by name",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			queueName := args[0]
			cli, err := pkg.NewSchedulerClient(serverAddr)
			if err != nil {
				fmt.Println("Failed to connect to server")
			}
			defer cli.Close()
			err = cli.CreateQueue(queueName)
			if err != nil {
				fmt.Println("Failed to create queue")
			}
		},
	}

	var testCmd = &cobra.Command{
		Use:   "test",
		Short: "test classic flow",
		Run: func(cmd *cobra.Command, args []string) {
			cli, err := pkg.NewSchedulerClient(serverAddr)
			if err != nil {
				fmt.Println("Failed to connect to server")
			}
			err = cli.CreateQueue("test")
			if err != nil {
				fmt.Println("Failed to create queue")
			}
			err = cli.PushItem("test", []byte("lorem ipsum"), time.Now().Add(5*time.Second).UnixMilli())
			if err != nil {
				fmt.Println("Failed to create item")
			}
			cli.Listen("test", func(message []byte) {
				fmt.Println("Recieved message: ", string(message))
			})
		},
	}

	var queueCmd = &cobra.Command{
		Use:   "queue",
		Short: "Manage queues",
	}

	rootCmd.AddCommand(serverCmd)
	queueCmd.AddCommand(listQueueCmd)
	queueCmd.AddCommand(deleteQueueCmd)
	queueCmd.AddCommand(createQueueCmd)

	rootCmd.Flags().StringVarP(&addr, "addr", "a", "localhost:8080", "gRPC server address")
	rootCmd.AddCommand(queueCmd)
	rootCmd.AddCommand(testCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
	}
}
