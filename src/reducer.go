package main

import (
	"GoExercise/src/common"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

type Reducer struct{}

func (s *Reducer) Compute(args *common.ReduceTask, reply *common.Reply) error {
	fmt.Printf("Reducer received task: %d\n", args.TaskID)
	reduceTask(args)
	reply.Status = "completed"
	return nil
}

func reduceTask(task *common.ReduceTask) {
	words := make(map[string]int)

	for i := 0; i < task.NumMappers; i++ {
		fileName := fmt.Sprintf("map_output_%d.txt", i)
		fileData, err := os.ReadFile(fileName)
		if err != nil {
			log.Fatalf("Failed to read file %s: %v", fileName, err)
		}

		lines := strings.Split(string(fileData), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			partitions := strings.Fields(line)
			word := partitions[0]
			count, _ := strconv.Atoi(partitions[1])
			words[word] += count
		}
	}

	outputFile := fmt.Sprintf("reduce_output_%d.tmp", task.TaskID)
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create file %s: %v", outputFile, err)
	}
	defer file.Close()

	for word, count := range words {
		fmt.Fprintf(file, "%s %d\n", word, count)
	}
	fmt.Printf("Reducer finished processing TaskID=%d, output=%s\n", task.TaskID, outputFile)
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: go run reducer.go <port>")
	}

	port := os.Args[1]
	addr := fmt.Sprintf("127.0.0.1:%s", port)

	reducer := new(Reducer)
	server := rpc.NewServer()
	err := server.Register(reducer)
	if err != nil {
		log.Fatalf("RPC error registering reducer")
	}
	log.Println("Reducer service registered successfully.")

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Listen error:", err)
	}

	go func() {
		for {
			server.Accept(listener)
		}
	}()
	select {}
}
