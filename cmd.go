package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ayushmd/delayedQ/pkg"
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

	var listQueueCmd = &cobra.Command{
		Use:   "list",
		Short: "List all queues",
		Run: func(cmd *cobra.Command, args []string) {
			cli := pkg.NewSchedulerClient("http://localhost:8080")
			cli.ListQueues()
			// qs, _ := ListAllQueues()
			// for _, q := range qs {
			// 	fmt.Println(q)
			// }
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
			cli := pkg.NewSchedulerClient("http://localhost:8080")
			cli.DeleteQueue(queueName)
			// err := DeleteQueue(queueName)
			// if err != nil {
			// 	log.Fatalf("Failed to delete queue: %v", err)
			// }
			// fmt.Printf("Deleted queue %s\n", queueName)
		},
	}

	var createQueueCmd = &cobra.Command{
		Use:   "create [queue_name]",
		Short: "Delete a queue by name",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			queueName := args[0]
			cli := pkg.NewSchedulerClient("http://localhost:8080")
			cli.InitQueue(queueName)
			// err := CreateQueue(queueName)
			// if err != nil {
			// 	log.Fatalf("Failed to create queue: %v", err)
			// }
			// fmt.Printf("Created queue: %s\n", queueName)
		},
	}

	var testCmd = &cobra.Command{
		Use:   "test",
		Short: "test classic flow",
		Run: func(cmd *cobra.Command, args []string) {
			cli := pkg.NewSchedulerClient("http://localhost:8080")
			cli.InitQueue("test")
			type TestInput struct {
				QueueName string `json:"queueName"`
				Data      string `json:"data"`
				Ttl       int    `json:"ttl"`
			}
			cli.Send(TestInput{
				QueueName: "test",
				Ttl:       int(time.Now().Add(5 * time.Second).UnixMilli()),
				Data:      "lorem ipsum",
			})
			cli.ListenQueue("test", func(message string) {
				fmt.Println("Recieved message: ", message)
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

	rootCmd.AddCommand(queueCmd)
	rootCmd.AddCommand(testCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
	}
}
