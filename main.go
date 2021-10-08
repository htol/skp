package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	var (
		bootstrap string
		topic     string
	)
	flag.StringVar(&bootstrap, "b", "localhost:9029", "kafka bootstrap url")
	flag.Parse()

	produceFlags := flag.NewFlagSet("produce", flag.ExitOnError)
	produceFlags.StringVar(&topic, "t", "", "topic name")
	listFlags := flag.NewFlagSet("list", flag.ExitOnError)

	args := flag.Args()
	switch args[0] {
	case "produce":
		produceFlags.Parse(args[1:])
		ctx := context.Background()
		produce(ctx, bootstrap, topic)
	case "list":
		listFlags.Parse(args[1:])
		topicList(bootstrap)
	default:
		log.Fatalf("[ERROR] unknown subcommand '%s', see help for more details.", args[0])
	}

}

func produce(ctx context.Context, bootstrap string, topic string) {
	partition := 0

	conn, err := kafka.DialLeader(ctx, "tcp", bootstrap, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func topicList(bootstrap string) {
	conn, err := kafka.Dial("tcp", bootstrap)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	for k := range m {
		fmt.Println(k)
	}
}
