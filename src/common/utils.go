package common

type MapTask struct {
	ChunkID   int
	ChunkData string
}

type ReduceTask struct {
	TaskID     int
	NumMappers int
}

type Reply struct {
	Status string
}
