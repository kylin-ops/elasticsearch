package main

import (
	"context"
	"fmt"
	"github.com/kylin-ops/elasticsearch"
)

func main() {
	es, err := elasticsearch.NewEsClient([]string{"http://10.100.201.189:9200"})
	fmt.Println(err)
	status := es.Client.ClusterHealth()
	fmt.Println(status.Do(context.Background()))
}
