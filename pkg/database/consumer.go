package database

import (
	"bufio"
	"log"
	"os"

	"github.com/gowerm123/jdb/pkg/shared"
)

type Consumer struct {
	target string
	udfs   []func(shared.Blob) shared.Blob
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
		line := scanner.Text()

		log.Println(line)
	}

	return nil
}
