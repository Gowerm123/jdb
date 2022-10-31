package jdbql

import (
	"fmt"

	"github.com/gowerm123/jdb/pkg/shared"
)

type InterpretterInstance struct {
	ptr               int
	tokens            []JdbToken
	outputInstruction shared.Instruction
	outputError       error
}

func InstanceWithInstructions(instructions []JdbToken) *InterpretterInstance {
	return &InterpretterInstance{
		ptr:    0,
		tokens: instructions,
	}
}

func (inst *InterpretterInstance) Error(fmtStr string, args ...any) {
	inst.outputError = fmt.Errorf(fmtStr, args...)
}

func (inst *InterpretterInstance) Evaluate() *InterpretterInstance {
	token := inst.tokens[inst.ptr]
	if token.TokenType != TOKEN_TYPE_SYM {
		inst.Error("unnkown sym %s, expected one of %v", string(token.contents), shared.Operations)
		return inst
	}
	operation, err := interrogateOperation(token)
	if err != nil {
		inst.Error(err.Error())
		return inst
	}

	inst.outputInstruction.Operation = operation
	inst.ptr++
	switch operation {
	case shared.JdbSelect:
		inst.selectExpressions()
	}

	return inst
}

func (inst *InterpretterInstance) Output() (*shared.Instruction, error) {
	return &inst.outputInstruction, inst.outputError
}

func interrogateOperation(token JdbToken) (string, error) {
	if !contains(shared.Operations, string(token.GetContents())) {
		return "", fmt.Errorf("not a known operation - %s", string(token.GetContents()))
	}
	return string(token.GetContents()), nil
}

func Interpret(instructions []JdbToken) (*shared.Instruction, error) {
	return InstanceWithInstructions(instructions).Evaluate().Output()
}
