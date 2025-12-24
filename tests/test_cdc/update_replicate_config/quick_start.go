package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func main() {
	const (
		clusterAAddr = `http://127.0.0.1:19530`
		clusterBAddr = `http://127.0.0.1:19531`
		clusterAID   = `cluster-a`
		clusterBID   = `cluster-b`
		pchannelNum  = 16
	)

	clsuterAPchannels := make([]string, 0, pchannelNum)
	clusterBPchannels := make([]string, 0, pchannelNum)
	for i := 0; i < pchannelNum; i++ {
		clsuterAPchannels = append(clsuterAPchannels, fmt.Sprintf("%s-rootcoord-dml_%d", clusterAID, i))
		clusterBPchannels = append(clusterBPchannels, fmt.Sprintf("%s-rootcoord-dml_%d", clusterBID, i))
	}
	clusterA := milvusclient.NewMilvusClusterBuilder(clusterAID).
		WithURI(clusterAAddr).
		WithPchannels(clsuterAPchannels...).
		Build()
	clusterB := milvusclient.NewMilvusClusterBuilder(clusterBID).
		WithURI(clusterBAddr).
		WithPchannels(clusterBPchannels...).
		Build()

	config := milvusclient.NewReplicateConfigurationBuilder().
		WithCluster(clusterA).
		WithCluster(clusterB).
		WithTopology(clusterAID, clusterBID).
		Build()

	updateReplicateConfigToCluster := func(clusterAddr string) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
			Address: clusterAddr,
		})
		if err != nil {
			log.Fatal(err)
		}
		err = cli.UpdateReplicateConfiguration(ctx, config)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Replicate configuration updated to cluster %s.\n", clusterAddr)
	}
	updateReplicateConfigToCluster(clusterAAddr)
	updateReplicateConfigToCluster(clusterBAddr)
}
