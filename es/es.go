package es

import (
	"fmt"
	"kayak/wok"

	"gopkg.in/olivere/elastic.v3"
)

//ElasticsearchClient represent an ElasticsearchClient
type ElasticsearchClient struct {
	Client  *elastic.Client
	ESIndex string
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

	ESClient.Client.Index().
		Index(ESClient.ESIndex).
		Type("wok_message").
		Id(m.UUID).
		BodyJson(m).
		Refresh(true).
		Do()
}
