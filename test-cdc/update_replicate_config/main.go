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
		sourceAddr = `http://172.18.50.5:19530`
		targetAddr = `http://172.18.50.5:19531`

		sourceClusterID = `by-dev1`
		targetClusterID = `by-dev2`

		pchannelNum = 16
	)

	sourcePchannels := make([]string, 0, pchannelNum)
	targetPchannels := make([]string, 0, pchannelNum)
	for i := 0; i < pchannelNum; i++ {
		sourcePchannels = append(sourcePchannels, fmt.Sprintf("%s-rootcoord-dml_%d", sourceClusterID, i))
		targetPchannels = append(targetPchannels, fmt.Sprintf("%s-rootcoord-dml_%d", targetClusterID, i))
	}

	// Use builder pattern to create cluster configuration (chained calls)
	sourceCluster := milvusclient.NewMilvusClusterBuilder(sourceClusterID).
		WithURI(sourceAddr).
		WithPchannels(sourcePchannels...).
		Build()

	targetCluster := milvusclient.NewMilvusClusterBuilder(targetClusterID).
		WithURI(targetAddr).
		WithPchannels(targetPchannels...).
		Build()

	// Use builder pattern to build replicate configuration
	config := milvusclient.NewReplicateConfigurationBuilder().
		WithCluster(sourceCluster).
		WithCluster(targetCluster).
		WithTopology(sourceClusterID, targetClusterID).
		Build()

	// Update replicate configuration
	updateFn := func(addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
			Address: addr,
		})
		if err != nil {
			log.Fatal(err)
		}
		err = cli.UpdateReplicateConfiguration(ctx, config)
		if err != nil {
			log.Printf("Failed to update replicate configuration: %v", err)
			return
		}

		log.Println("Replicate source configuration updated successfully")
	}

	updateFn(sourceAddr)
	updateFn(targetAddr)
}
