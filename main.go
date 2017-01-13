package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"

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
	var wg sync.WaitGroup
	messageChannel := make(chan sarama.ConsumerMessage)
	wg.Add(len(partitions))
	signalsMain := make(chan os.Signal, 1)
	signal.Notify(signalsMain, os.Interrupt)
	for _, consumer := range partitionsConsumerList {
		go readPartition(consumer, &wg, messageChannel)
	}
	msgCount := 0
	for {
		select {
		case msg := <-messageChannel:
			fmt.Println("VALUE :", msg.Value)
			msgCount++
		case <-signalsMain:
			fmt.Println("Finished reading")
			fmt.Println("Waitnig all consumers ...")
			fmt.Println("Msg recevied : ", msgCount)
			wg.Wait()
			os.Exit(0)
		}
	}
}

func readPartition(consumer *sarama.PartitionConsumer, wg *sync.WaitGroup, messageChannel chan sarama.ConsumerMessage) (int, error) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	asyncRead(consumer, signals, wg, messageChannel)
	return 0, nil
}

func asyncRead(consumer *sarama.PartitionConsumer, signals chan os.Signal, wg *sync.WaitGroup, messageChannel chan sarama.ConsumerMessage) {
	for {
		select {
		case err := <-(*consumer).Errors():
			fmt.Println(err)
		case msg := <-(*consumer).Messages():
			messageChannel <- *msg
		case <-signals:
			fmt.Println("Interrupt is detected")
			wg.Done()
			return
		}
	}
}
