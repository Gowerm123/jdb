package database

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/gowerm123/jdb/pkg/shared"
)

type Consumer struct {
	target string
	udfs   []func(shared.Blob) shared.Blob
	buffer []shared.Blob
}

func NewConsumer(target string, udfs ...func(shared.Blob) shared.Blob) Consumer {
	return Consumer{
		target: target,
		udfs:   udfs,
	}
}

func (cons *Consumer) ConsumeAll() []shared.Blob {
	filePath := ResolveFile(cons.target)
	file, err := os.Open(filePath)

	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		println("scanning")
		line := scanner.Text()
		if line == "" {
			continue
		}

		var blob shared.Blob
		json.Unmarshal([]byte(line), &blob)
		for _, udf := range cons.udfs {
			if udf == nil {
				continue
			}
			blob = udf(blob)
			if blob == nil {
				break
			}
		}
		if blob != nil {
			cons.buffer = append(cons.buffer, blob)
		}
	}

	return nil
}

func (cons *Consumer) ReadAll() []shared.Blob {
	blobs := cons.buffer

	cons.buffer = []shared.Blob{}

	return blobs
}
