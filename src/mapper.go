package main

import (
	"GoExercise/src/common"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"regexp"
	"strings"
)

type Mapper struct{}

func (m *Mapper) ServeRequest(args *common.MapTask, reply *common.Reply) error {
	fmt.Printf("Mapper received task: ChunkID=%d\n", args.ChunkID)
	mapChunk(args)
	reply.Status = "completed"
	return nil
}

func mapChunk(task *common.MapTask) {
	re := regexp.MustCompile(`[^\w\s]`) // lascia fuori i caratteri non alfanumerici

	words := strings.Fields(task.ChunkData)
	wordCount := make(map[string]int)

	for _, word := range words {

		cleanedWord := strings.ToLower(word)
		cleanedWord = re.ReplaceAllString(cleanedWord, "")

		if cleanedWord != "" {
			wordCount[cleanedWord]++
		}
	}

	outputFile := fmt.Sprintf("map_output_%d.txt", task.ChunkID)
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create file %s: %v", outputFile, err)
	}
	defer file.Close()

	for word, count := range wordCount {
		fmt.Fprintf(file, "%s %d\n", word, count)
	}
	fmt.Printf("Mapper finished processing ChunkID=%d, output=%s\n", task.ChunkID, outputFile)
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: go run mapper.go <port>")
	}

	port := os.Args[1]
	addr := fmt.Sprintf("127.0.0.1:%s", port)

	mapper := new(Mapper)
	server := rpc.NewServer()
	err := server.Register(mapper)
	if err != nil {
		log.Fatalf("RPC error registering mapper")
	}
	log.Println("Mapper service registered successfully.")

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	defer listener.Close()

	log.Printf("Serving RPC on %s", addr)

	go func() {
		for {
			server.Accept(listener)
		}
	}()
	select {}

}
