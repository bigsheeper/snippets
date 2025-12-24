package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func testSwitchover() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	mode := os.Args[1]
	if mode != "init" && mode != "switch" {
		printUsage()
		os.Exit(1)
	}

	const (
		clusterAAddr = `http://172.18.50.5:19530`
		clusterBAddr = `http://172.18.50.5:19531`

		clusterAID = `by-dev1`
		clusterBID = `by-dev2`

		pchannelNum = 16
	)

	clsuterAPchannels := make([]string, 0, pchannelNum)
	clusterBPchannels := make([]string, 0, pchannelNum)
	for i := 0; i < pchannelNum; i++ {
		clsuterAPchannels = append(clsuterAPchannels, fmt.Sprintf("%s-rootcoord-dml_%d", clusterAID, i))
		clusterBPchannels = append(clusterBPchannels, fmt.Sprintf("%s-rootcoord-dml_%d", clusterBID, i))
	}

	// Use builder pattern to create cluster configuration (chained calls)
	clusterA := milvusclient.NewMilvusClusterBuilder(clusterAID).
		WithURI(clusterAAddr).
		WithPchannels(clsuterAPchannels...).
		Build()

	clusterB := milvusclient.NewMilvusClusterBuilder(clusterBID).
		WithURI(clusterBAddr).
		WithPchannels(clusterBPchannels...).
		Build()

	// Use builder pattern to build replicate configuration
	var config *commonpb.ReplicateConfiguration
	if mode == "init" {
		fmt.Println("Init replicate configuration: A -> B")
		config = milvusclient.NewReplicateConfigurationBuilder().
			WithCluster(clusterA).
			WithCluster(clusterB).
			WithTopology(clusterAID, clusterBID).
			Build()
	} else {
		fmt.Println("Switch primary-standby: B -> A")
		config = milvusclient.NewReplicateConfigurationBuilder().
			WithCluster(clusterA).
			WithCluster(clusterB).
			WithTopology(clusterBID, clusterAID).
			Build()
	}

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

	updateFn(clusterAAddr)
	updateFn(clusterBAddr)
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  ./main init   - Init replicate configuration: A -> B")
	fmt.Println("  ./main switch - Switch primary-standby: B -> A")
	fmt.Println("")
}
