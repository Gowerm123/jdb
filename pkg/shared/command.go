package shared

import (
	"net/http"
)

var (
	IdChannel     chan int
	RespChannels  []chan string
	CmdChannels   []chan Instruction
	TableMappings map[string]int
	roundRobinPtr int = 0
)

type Instruction struct {
	Operation string
	Targets   []string
	Tags      map[string]interface{}
}

type Tag struct {
	Key   string
	Value interface{}
}

type CommandContext struct {
	req *http.Request
	rw  http.ResponseWriter
	cmd Command
}

func CreateContext(req *http.Request, rw http.ResponseWriter, cmd Command) CommandContext {
	cc := CommandContext{}

	cc.req = req
	cc.rw = rw
	cc.cmd = cmd

	return cc
}

func (cc *CommandContext) Execute() error {
	for _, instruction := range cc.cmd {
		cc.cmd.execute(instruction)
	}
	return nil
}

type Command []Instruction

func (cmd *Command) AddInstruction(inst Instruction) {
	*cmd = append(*cmd, inst)
}

func (cmd *Command) execute(inst Instruction) int {
	return forward(inst)
}

func (inst *Instruction) AddTag(tag Tag) {
	if inst.Tags == nil {
		inst.Tags = make(map[string]interface{})
	}
	if tag.Key == "targets" {
		inst.Targets = tag.Value.([]string)
	}
	inst.Tags[tag.Key] = tag.Value
}

func toBlobList(tables map[string]TableEntry) (blobs []Blob) {
	for key := range tables {
		if key == "" {
			continue
		}
		blobs = append(blobs, Blob{"tableName": key})
	}
	return blobs
}

func forward(inst Instruction) (chId int) {
	CmdChannels[roundRobinPtr] <- inst

	chId = roundRobinPtr
	roundRobinPtr++
	if roundRobinPtr >= len(CmdChannels) {
		roundRobinPtr = 0
	}
	return chId
}
