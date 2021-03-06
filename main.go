package main

import (
	"flag"
	"fmt"
	"kayak/es"
	"os"
	"os/signal"

	"gitlab.botsunit.com/msaas/wok-go/wok"

	sarama "github.com/Shopify/sarama"
)

var (
	kafka              = flag.String("b", "localhost:9092", "kafka url format IP:PORT")
	topic              = flag.String("t", "bots_events", "topic name ex : bots_events")
	offset             = flag.String("o", "new", "offset to leave from new for newest, old for oldest")
	elasticsearchURL   = flag.String("e", "http://localhost:9200", "ElasticSearch URL")
	elasticsearchIndex = flag.String("i", "wok_message", "ElasticSearch Index Name")
	versionFlag        = flag.Bool("version", false, "Print version of the program")
	version            string
	offsetChoosen      int64
)

func main() {
	flag.Parse()
	if *versionFlag {
		fmt.Println("Kayak Version ", version)
		os.Exit(0)
	}
	if *offset == "new" {
		offsetChoosen = sarama.OffsetNewest
	} else {
		offsetChoosen = sarama.OffsetOldest
	}
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// Specify brokers address. This is default one
	brokers := []string{*kafka}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		panic(err)
	}
	mainConsumer, err := sarama.NewConsumerFromClient(client)
	// Create new consumer
	//master, err := sarama.NewConsumer(brokers, config)

	if err != nil {
		panic(err)
	}

	defer func() {
		if errClose := mainConsumer.Close(); err != nil {
			panic(errClose)
		}
	}()

	fmt.Println("Listing partitions ...")
	partitions, errPart := mainConsumer.Partitions(*topic)
	if errPart != nil {
		panic(errPart)
	}
	fmt.Println("Found partitions : ", partitions)

	partitionsConsumerList := make([]*sarama.PartitionConsumer, len(partitions))
	for idx, part := range partitions {
		consumer, err := mainConsumer.ConsumePartition(*topic, part, offsetChoosen)
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

	esClient := es.NewElasticsearchClient(*elasticsearchURL, *elasticsearchIndex)

	for {
		select {
		case msg := <-messageChannel:
			wm := wok.Message{Binary: msg.Value}
			wm.DecodeMessage()
			genericMessage := wm.ToGenericMessage(msg.Offset, msg.Partition)
			fmt.Println(genericMessage.Stdout())
			esClient.ForwardMessage(genericMessage)
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
