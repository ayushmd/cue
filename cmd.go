package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/ayushmd/delayedQ/pkg/cuecl"
	"github.com/spf13/cobra"
)

func runCmd() {
	var addr string
	serverAddr := fmt.Sprintf("%s:%d", addr, cfg.Port)
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
			cli, err := cuecl.NewCueClient(serverAddr)
			if err != nil {
				fmt.Println("Failed to connect server")
			}
			defer cli.Close()
			qs, err := cli.ListQueues()
			if err != nil {
				fmt.Println("Failed to list queues")
			}
			// qs, _ := ListAllQueues()
			if len(qs) == 0 {
				fmt.Println("No queue's found")
			} else {
				fmt.Println("Queue:")
				for _, q := range qs {
					fmt.Println(q)
				}
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
			cli, err := cuecl.NewCueClient(serverAddr)
			if err != nil {
				fmt.Println("Failed to connect server")
			}
			defer cli.Close()
			err = cli.DeleteQueue(queueName)
			if err != nil {
				fmt.Println("Queue does not exsists")
			} else {
				fmt.Println("Deleted queue")
			}
		},
	}

	var createQueueCmd = &cobra.Command{
		Use:   "create [queue_name]",
		Short: "Delete a queue by name",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			queueName := args[0]
			cli, err := cuecl.NewCueClient(serverAddr)
			if err != nil {
				fmt.Println("Failed to connect to server")
			}
			defer cli.Close()
			err = cli.CreateQueue(queueName)
			if err != nil {
				fmt.Println("Failed to create queue")
			} else {
				fmt.Println("Created queue")
			}
		},
	}

	var listenCmd = &cobra.Command{
		Use:   "listen",
		Short: "listen to queue and acks",
		Run: func(cmd *cobra.Command, args []string) {
			cli, err := cuecl.NewCueClient(serverAddr)
			if err != nil {
				fmt.Println("Failed to connect to server")
			}
			defer cli.Close()
			var queueName string
			if len(args) == 0 {
				queueName = "test"
			} else {
				queueName = args[0]
			}
			err = cli.CreateQueue(queueName)
			if err != nil {
				fmt.Println("Failed to create queue")
			}
			ch, err := cli.Listen(queueName)
			if err != nil {
				fmt.Println("Failed to create Listen")
			}
			for data := range ch {
				fmt.Println("Recieved: ", string(data), time.Now().UnixMilli())
			}
		},
	}
	var putCmd = &cobra.Command{
		Use:   "put",
		Short: "puts a item in queue",
		Run: func(cmd *cobra.Command, args []string) {
			cli, err := cuecl.NewCueClient(serverAddr)
			if err != nil {
				fmt.Println("Failed to connect to server")
			}
			defer cli.Close()
			var ttl int64
			if len(args) == 0 {
				ttl = time.Now().Add(15 * time.Second).UnixMilli()
			} else {
				num, err := strconv.Atoi(args[0])
				if err != nil {
					fmt.Println("Conversion error:", err)
					return
				}
				ttl = time.Now().Add(time.Duration(num) * time.Millisecond).UnixMilli()
			}
			err = cli.PushItem("test.*", []byte(fmt.Sprintf("{'data':'test data', 'createdAt': '%d'}", time.Now().UnixMilli())), ttl)
			if err != nil {
				fmt.Println("Failed to create item ", err)
			}
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
	if debug {
		rootCmd.AddCommand(listenCmd)
		rootCmd.AddCommand(putCmd)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
	}
}
