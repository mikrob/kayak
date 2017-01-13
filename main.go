package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	sarama "github.com/Shopify/sarama"
)

var (
	kafka = flag.String("b", "localhost:9092", "kafka url format IP:PORT")
	topic = flag.String("t", "bots_events", "topic name ex : bots_events")
)

func main() {
	flag.Parse()
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Specify brokers address. This is default one
	brokers := []string{*kafka}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if errClose := master.Close(); err != nil {
			panic(errClose)
		}
	}()

	fmt.Println("Listing partitions ...")
	partitions, errPart := master.Partitions(*topic)
	if errPart != nil {
		panic(errPart)
	}
	fmt.Println("Found partitions : ", partitions)

	partitionsConsumerList := make([]*sarama.PartitionConsumer, len(partitions))
	for idx, part := range partitions {
		consumer, err := master.ConsumePartition(*topic, part, sarama.OffsetOldest)
		if err != nil {
			panic(err)
		}
		partitionsConsumerList[idx] = &consumer
	}

	doneChannel := make(chan struct{})
	messageChannel := make(chan sarama.ConsumerMessage)

	runningCount := 0
	for _, consumer := range partitionsConsumerList {
		runningCount++
		go readPartition(consumer, doneChannel, messageChannel)
	}
	msgCount := 0

	for {
		select {
		case msg := <-messageChannel:
			fmt.Println("VALUE :", msg.Value)
			msgCount++
		case <-doneChannel:
			runningCount--
			if runningCount == 0 {
				fmt.Println("Msg recevied : ", msgCount)
				os.Exit(0)
			}
		}
	}
}

func readPartition(consumer *sarama.PartitionConsumer, doneChannel chan struct{}, messageChannel chan sarama.ConsumerMessage) (int, error) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	asyncRead(consumer, signals, doneChannel, messageChannel)
	return 0, nil
}

func asyncRead(consumer *sarama.PartitionConsumer, signals chan os.Signal, doneChannel chan struct{}, messageChannel chan sarama.ConsumerMessage) {
	for {
		select {
		case err := <-(*consumer).Errors():
			fmt.Println(err)
		case msg := <-(*consumer).Messages():
			messageChannel <- *msg
		case <-signals:
			fmt.Println("Interrupt is detected")
			doneChannel <- struct{}{}
			return
		}
	}
}
