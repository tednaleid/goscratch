package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: kafka-topics-lister <broker:port>")
		os.Exit(1)
	}

	// Create config
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0 // Kafka 3.0+ version

	// Create client
	brokers := []string{os.Args[1]}
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}
	defer client.Close()

	// Get topics
	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Error getting topics: %v", err)
	}

	// Print topics
	if len(topics) == 0 {
		fmt.Println("No topics found")
		return
	}

	fmt.Println("Topics:")
	fmt.Println(strings.Join(topics, "\n"))
}
