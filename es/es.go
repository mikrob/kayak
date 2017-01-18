package es

import (
	"fmt"

	"gitlab.botsunit.com/msaas/wok-go/wok"

	"time"

	"gopkg.in/olivere/elastic.v3"
)

//ElasticsearchClient represent an ElasticsearchClient
type ElasticsearchClient struct {
	Client  *elastic.Client
	ESIndex string
}

func logstashIndex() string {
	now := time.Now()
	return fmt.Sprintf("logstash-%d.%.2d.%.2d", now.Year(), now.Month(), now.Day())
}

//NewElasticsearchClient allow to create an ElasticsearchClient
func NewElasticsearchClient(URL string, index string) ElasticsearchClient {
	// Create a client
	client, err := elastic.NewClient()
	elastic.SetMaxRetries(10)
	elastic.SetURL(URL)
	if err != nil {
		fmt.Println("Error while initializing Elasticsearch client : ", err.Error())
	}
	client.CreateIndex(index).Do()
	return ElasticsearchClient{Client: client, ESIndex: index}
}

//ForwardMessage is used to forward a message in elasticsearch
func (ESClient *ElasticsearchClient) ForwardMessage(m wok.GenericMessage) {
	// Create an index
	if ESClient.ESIndex == "logstash" {
		ESClient.ESIndex = logstashIndex()
	}

	ESClient.Client.Index().
		Index(ESClient.ESIndex).
		Type("wok_message").
		Id(m.UUID).
		BodyJson(m).
		Refresh(true).
		Do()
}
