package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const (
	milvusAddr     = "http://localhost:19530"
	collectionName = "hello_milvus"
	dim            = 1024
	numEntities    = 1
	numConcurrent  = 1
	numInsert      = 100
)

func dropCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		panic(err)
	}
	defer cli.Close(ctx)

	err = cli.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	if err != nil {
		panic(err)
	}
	// fmt.Println("Collection dropped")
}

func createCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		panic(err)
	}
	defer cli.Close(ctx)

	err = cli.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, dim))
	if err != nil {
		panic(err)
	}

	// fmt.Println("Collection created")
}

func insert() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		panic(err)
	}

	defer cli.Close(ctx)

	vectors := make([][]float32, numEntities)
	for i := 0; i < numEntities; i++ {
		vec := make([]float32, dim)
		for k := 0; k < dim; k++ {
			vec[k] = float32(i*dim+k) / float32(numEntities*dim)
		}
		vectors[i] = vec
	}

	for i := 0; i < numInsert; i++ {
		_, err := cli.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).
			WithFloatVectorColumn("vector", dim, vectors),
		)
		if err != nil {
			panic(err)
		}
		// fmt.Printf("Insert %d success, result: %v\n", i+1, resp)
	}
}

func main() {
	dropCollection()
	createCollection()
	wg := sync.WaitGroup{}
	startTime := time.Now()
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// insertStart := time.Now()
			insert()
			// insertEnd := time.Now()
			// fmt.Printf("Goroutine %d finished in %.2f ms\n", idx, float64(insertEnd.Sub(insertStart).Milliseconds()))
		}(i)
	}
	wg.Wait()

	endTime := time.Now()
	totalDurationMs := float64(endTime.Sub(startTime).Milliseconds())
	// 吞吐 = dim * 4 * rows / 时间
	rows := int64(numConcurrent) * int64(numInsert) * int64(numEntities)
	bytes := int64(dim) * 4 * rows
	throughput := float64(bytes) / 1024.0 / 1024.0 / (totalDurationMs / 1000.0)
	// fmt.Println("All inserts done")
	QPS := float64(numConcurrent) * float64(numInsert) / (totalDurationMs / 1000.0)
	fmt.Printf("Total time: %.2f ms, Total bytes: %d, Throughput: %.4f MB/s, QPS: %.4f, concurrency: %d\n", totalDurationMs, bytes, throughput, QPS, numConcurrent)
}
