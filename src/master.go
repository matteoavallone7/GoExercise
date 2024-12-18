package main

import (
	"GoExercise/src/common"
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	chunkSize = 5
	reducers  = 3
)

type Worker struct {
	IP       string
	Mappers  []string
	Reducers []string
}

type Config struct {
	Workers []Worker
}

func readConfigFile(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}

	var config Config
	err = json.Unmarshal(data, &config)
	return config, err
}

func readAndSplitFile(filename string, chunkSize int) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var chunks []string
	var currentChunk []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		currentChunk = append(currentChunk, scanner.Text())
		if len(currentChunk) >= chunkSize {
			chunks = append(chunks, strings.Join(currentChunk, "\n"))
			currentChunk = []string{}
		}
	}

	if len(currentChunk) > 0 {
		chunks = append(chunks, strings.Join(currentChunk, "\n"))
	}

	return chunks, scanner.Err()
}

func assignMapTask(chunk int, data string, mapper string) {
	addr := "127.0.0.1:" + mapper
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to connect to mapper at %s: %v", addr, err)
	}

	args := common.MapTask{
		ChunkID:   chunk,
		ChunkData: data,
	}

	var reply = common.Reply{}

	err = client.Call("Mapper.ServeRequest", args, &reply)
	if err != nil {
		log.Fatalf("RPC error with mapper at %s: %v", mapper, err)
	}

}

func assignReduceTask(taskID, numMappers int, reducer string) {
	addr := "127.0.0.1:" + reducer
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to connect to reducer at %s: %v", reducer, err)
	}

	args := common.ReduceTask{
		TaskID:     taskID,
		NumMappers: numMappers,
	}

	var reply = common.Reply{}

	err = client.Call("Reducer.Compute", &args, &reply)
	if err != nil {
		log.Fatalf("RPC error with reducer at %s: %v", reducer, err)
	}
}

func mergeOutputs(finalOutput string) {
	words := make(map[string]int)

	for i := 0; i < reducers; i++ {
		fileName := fmt.Sprintf("reduce_output_%d.tmp", i)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Failed to open reducer output file %s: %v", fileName, err)
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			partitions := strings.Fields(line)
			if len(partitions) != 2 {
				log.Printf("Invalid line format in %s: %s", fileName, line)
				continue
			}
			word := partitions[0]
			count, err := strconv.Atoi(partitions[1])
			if err != nil {
				log.Printf("Invalid count value in %s: %s", fileName, line)
				continue
			}
			words[word] += count
		}

		file.Close()
		os.Remove(fileName)
	}

	outputFile, err := os.Create(finalOutput)
	if err != nil {
		log.Fatalf("Failed to create final output file: %v", err)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	for word, count := range words {
		_, err := writer.WriteString(fmt.Sprintf("%s %d\n", word, count))
		if err != nil {
			log.Fatalf("Failed to write final output file: %v", err)
		}
	}

	writer.Flush()
	fmt.Println("Final output file created:", finalOutput)
}

func main() {

	config, err := readConfigFile("tsconfig.json")
	if err != nil {
		fmt.Println("Error reading worker config:", err)
		return
	}

	chunks, err := readAndSplitFile("persuasion.txt", chunkSize)
	if err != nil {
		log.Fatalf("Failed to read input file: %v", err)
	}

	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		mapperIndex := i % len(config.Workers[0].Mappers) // crea un'assegnazione circolare dei chunk
		mapper := config.Workers[0].Mappers[mapperIndex]
		go func(chunkID int, chunkData string) {
			defer wg.Done()
			assignMapTask(chunkID, chunkData, mapper)
		}(i, chunk)
	}
	wg.Wait()
	fmt.Println("All map tasks are completed.")

	for i := 0; i < reducers; i++ {
		wg.Add(1)
		reducerIndex := i % len(config.Workers[0].Reducers)
		reducer := config.Workers[0].Reducers[reducerIndex]
		go func(taskID int, addr string) {
			defer wg.Done()
			assignReduceTask(taskID, len(chunks), addr)
		}(i, reducer)
	}
	wg.Wait()
	fmt.Println("All reduce tasks are completed.")

	mergeOutputs("final_output.txt")
}
