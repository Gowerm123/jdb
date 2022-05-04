package database

import (
	"log"

	"github.com/gowerm123/jdb/pkg/shared"
)

type Consumer struct {
	target       string
	processors   map[string]func(shared.Blob) shared.Blob
	returnFields []string
}

func NewConsumer(table string, processors map[string]func(shared.Blob) shared.Blob, returnFields []string) Consumer {
	return Consumer{
		target:       table,
		processors:   processors,
		returnFields: returnFields,
	}
}

func (cons *Consumer) Consume() []shared.Blob {
	path := truePath(cons.target)
	log.Println(path)
	return []shared.Blob{}
}
