package server

import (
	"github.com/gowerm123/jdb/pkg/shared"
)

var (
	Channels      []chan shared.Instruction
	tableMappings map[string]int

	quit chan bool
)

func StartWorkers(n int) {
	Channels = make([]chan shared.Instruction, n)
	tableMappings = make(map[string]int)

	for i := 0; i < n; i++ {
		go listen(Channels[i])
	}
}

func listen(ch chan shared.Instruction) {
	for {
		select {
		case <-quit:
			return
		default:
			checkForTasks(ch)
		}
	}
}

func checkForTasks(ch chan shared.Instruction) {
	var inst = <-ch

}
