package elasticsearch

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
)

var ctx = context.Background()

func NewEsClient(address []string) (*Elasticsearch, error) {
	client, err := elastic.NewClient(elastic.SetURL(address...))
	if err != nil {
		return nil, err
	}
	bulk := client.Bulk()
	return &Elasticsearch{Client: client, Bulk: bulk}, nil
}

type Elasticsearch struct {
	Client *elastic.Client
	Bulk   *elastic.BulkService
}

func (e *Elasticsearch) IndexCreate(index string, shards, replicas int) error {
	mapping := fmt.Sprintf(`{
		"settings":{
			"number_of_shards":%d,
			"number_of_replicas":%d
		}
	}`, shards, replicas)
	_, err := e.Client.CreateIndex(index).BodyString(mapping).Do(ctx)
	return err
}

func (e *Elasticsearch) IndexAddAlias(index, alias string) error {
	_, err := e.Client.Alias().Add(index, alias).Do(ctx)
	return err
}

func (e *Elasticsearch) IndexExist(index string) (bool, error) {
	return e.Client.IndexExists(index).Do(ctx)
}

func (e *Elasticsearch) IndexDelete(index string) error {
	_, err := e.Client.DeleteIndex(index).Do(ctx)
	return err
}

func (e *Elasticsearch) DocsInsertBulk(index string, docs []interface{}) error {
	for _, doc := range docs {
		e.Bulk.Add(elastic.NewBulkCreateRequest().Index(index).Doc(doc))
	}
	_, err := e.Bulk.Do(ctx)
	return err
}

func (e *Elasticsearch) DocsQueryDelete(index, query string) (int64, error) {
	resp, err := e.Client.DeleteByQuery().Index(index).QueryString(query).Do(ctx)
	if err != nil {
		return 0, err
	}
	return resp.Deleted, err
}

func (e *Elasticsearch) DocsQuery(index, query string) (*elastic.SearchResult, error) {
	return e.Client.Search().Index(index).Query(elastic.NewQueryStringQuery(query)).Do(ctx)
}

func (e *Elasticsearch) IndexRolloverAddCondition(alias, maxAge string, maxDocs int) error {
	_, err := elastic.NewIndicesRolloverService(e.Client).Conditions(map[string]interface{}{
		"max_age":  maxAge,
		"max_docs": maxDocs,
	}).Alias(alias).Do(ctx)
	return err
}

func (e *Elasticsearch) IndexTemplateCreate(tmplName, index, alias string, shards, replicas int) error {
	tmpl := fmt.Sprintf(`{
		"index_patterns":["%s*"],
		"settings":{
			"number_of_shards":%d,
			"number_of_replicas":%d
		},
		  	"aliases": {
      			"%s":{}
  		}
	}`, index, shards, replicas, alias)
	_, err := e.Client.IndexPutTemplate(tmplName).BodyString(tmpl).Do(ctx)
	return err
}

func (e *Elasticsearch) IndexTemplateDelete(tmplName string) error {
	_, err := e.Client.IndexDeleteTemplate(tmplName).Do(ctx)
	return err
}

func (e *Elasticsearch) IndexTemplateExist(tmplName string) (bool, error) {
	return e.Client.IndexTemplateExists(tmplName).Do(ctx)
}
