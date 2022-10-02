package jdbql

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gowerm123/jdb/pkg/shared"
)

var (
	rawContents string
	truePtr     int
	iterPtr     int
	_cmd        shared.Command
	tokenBuffer []string
	tagBuffer   []shared.Tag
	prevToken   string
	currToken   string

	activeRequest *http.Request
	activeWriter  http.ResponseWriter

	keyWords []string = []string{
		shared.JdbSelect,
		shared.JdbCreate,
		shared.JdbDrop,
		shared.JdbInsert,
		shared.JdbDelete,
		shared.JdbFrom,
		shared.JdbTable,
		shared.JdbAs,
		shared.JdbInto,
		shared.JdbValues,
		shared.JdbDescribe,
	}
)

func AssignParserActives(req *http.Request, rw http.ResponseWriter) {
	activeRequest = req
	activeWriter = rw
}

func Parse(command string) {
	reset()
	rawContents = command
	nextToken(false)
	accept()
}

func accept() {
	if truePtr >= len(rawContents) {
		if len(tokenBuffer) > 0 || len(tagBuffer) > 0 {
			_cmd.AddInstruction(parseFromTokenBuffer())
		}
		context := shared.CreateContext(activeRequest, activeWriter, _cmd)
		err := context.Execute()
		if err != nil {
			panic(err)
		}
		return
	}
	switch currToken {
	case shared.JdbSelect:
		addToTokenBuffer(shared.JdbSelect)
		optional(shared.SELECT_COLUMNS)
		nextToken(false)
		expect(shared.JdbFrom)
	case shared.JdbFrom:
		optional("targets")
		nextToken(false)
		accept()
	case shared.JdbCreate:
		addToTokenBuffer(shared.JdbCreate)
		nextToken(false)
		expect(shared.JdbTable)
	case shared.JdbTable:
		nextToken(false)
		ident()
		accept()
	case shared.JdbAs:
		println(peekNext())
		if peekNext() == shared.JdbLoad {
			nextToken(false)
			accept()
		} else {
			schema := schema()
			nextToken(false)
			tagBuffer = append(tagBuffer, shared.Tag{Key: "schema", Value: schema})
			accept()
		}
	case shared.JdbDrop:
		addToTokenBuffer(shared.JdbDrop)
		nextToken(false)
		expect(shared.JdbTable)
	case shared.JdbDescribe:
		addToTokenBuffer(shared.JdbDescribe)
		nextToken(false)
		ident()
		accept()
	case shared.JdbInsert:
		addToTokenBuffer(shared.JdbInsert)
		nextToken(false)
		expect(shared.JdbInto)
	case shared.JdbInto:
		nextToken(false)
		ident()
		expect(shared.JdbValues)
	case shared.JdbValues:
		values := values()
		tagBuffer = append(tagBuffer, shared.Tag{Key: "values", Value: values})
		accept()
	case shared.JdbWhere:
		nextToken(false)
		predicate()
	case shared.JdbPartitioned:
		nextToken(false)
		expect(shared.JdbOn)
	case shared.JdbOn:
		switch prevToken {
		case shared.JdbPartitioned:
			optional(shared.PARTITION_COLUMNS)
			accept()
		}
	case shared.JdbList:
		addToTokenBuffer(shared.JdbList)
		nextToken(false)
		ident()
		accept()
	case shared.JdbGroup:
		nextToken(false)
		expect(shared.JdbBy)
	case shared.JdbBy:
		optional(shared.GROUP_BY_COLUMNS)
		accept()
		break
	case shared.JdbLoad:
		optional(shared.LOAD_TARGETS)
		accept()
	default:
		fatal("unexpected token", currToken)
	}
}

func expect(command string) {
	if currToken != command {
		fatal("unexpected token", currToken, "expected", command)
	}
	accept()
}

func nextToken(isIdent bool) {
	buff := ""
	if truePtr >= len(rawContents) {
		return
	}
	if rawContents[truePtr] == '\'' {
		truePtr++
		for truePtr < len(rawContents) && rawContents[truePtr] != '\'' {
			buff += string(rawContents[truePtr])
			truePtr++
		}
	} else {
		for truePtr < len(rawContents) && (rawContents[truePtr] != ' ') {
			buff += string(rawContents[truePtr])
			truePtr++
		}
		truePtr++
	}
	if !isIdent {
		prevToken = currToken
	}
	currToken = buff
}

func peekNext() string {
	buff, tmpPtr := "", truePtr
	if tmpPtr >= len(rawContents) {
		return ""
	}
	if rawContents[tmpPtr] == '\'' {
		tmpPtr++
		for tmpPtr < len(rawContents) && rawContents[tmpPtr] != '\'' {
			buff += string(rawContents[tmpPtr])
			tmpPtr++
		}
	} else {
		for tmpPtr < len(rawContents) && (rawContents[tmpPtr] != ' ') {
			buff += string(rawContents[tmpPtr])
			tmpPtr++
		}
		tmpPtr++
	}
	return buff
}

func reset() {
	iterPtr = 0
	truePtr = 0
	_cmd = shared.Command{}
	tokenBuffer = []string{}
	tagBuffer = []shared.Tag{}
	currToken = ""
	prevToken = ""
}

func optional(name string) {
	options := []string{}
	tmpPtr := truePtr
	for {
		token := ""
		for tmpPtr < len(rawContents) && rawContents[tmpPtr] != ',' && rawContents[tmpPtr] != ' ' {
			token += string(rawContents[tmpPtr])
			tmpPtr++
		}

		if isKeyword(token) {
			break
		}
		if token != "" {
			options = append(options, token)
		}
		if tmpPtr >= len(rawContents) || rawContents[tmpPtr] == ' ' {
			tmpPtr++
			break
		}

		tmpPtr++
		if rawContents[tmpPtr] == ' ' {
			tmpPtr++
		}
	}
	addToTagBuffer(name, options)
	truePtr = tmpPtr
}

func ident() {
	var val []string = []string{}
	for _, tag := range tagBuffer {
		if tag.Key == "targets" {
			val = tag.Value.([]string)
		}
	}
	addToTagBuffer("targets", append(val, currToken))
	nextToken(true)
}

func isKeyword(cmd string) bool {
	return contains(keyWords, cmd)
}

func schema() shared.Schema {
	lbrPointer := 1
	tempPtr := truePtr + 1
	for tempPtr < len(rawContents) && lbrPointer > 0 {
		if rawContents[tempPtr] == '{' {
			lbrPointer++
		} else if rawContents[tempPtr] == '}' {
			lbrPointer--
		}
		tempPtr++
	}
	subStr := rawContents[truePtr:tempPtr]
	truePtr = tempPtr
	var schema shared.Schema
	if err := json.Unmarshal([]byte(subStr), &schema); err != nil {
		panic(fmt.Errorf("faild to parse json structure - %s", err.Error()))
	}

	return schema
}

func values() []shared.Blob {
	tmpPtr := truePtr + 1
	lPtr := tmpPtr
	for rawContents[lPtr] != '{' {
		lPtr--
	}

	brackCtr := 1
	for tmpPtr < len(rawContents) {
		if brackCtr == 0 && rawContents[tmpPtr] == ' ' {
			truePtr = tmpPtr + 1
			break
		}
		if rawContents[tmpPtr] == '{' {
			brackCtr++
		} else if rawContents[tmpPtr] == '}' {
			brackCtr--
		}

		tmpPtr++
	}

	blobs := []shared.Blob{}

	subStr := rawContents[truePtr:]
	split := strings.Split(subStr, "@@")
	for _, blobStr := range split {
		var blob shared.Blob
		json.Unmarshal([]byte(blobStr), &blob)
		blobs = append(blobs, blob)
	}

	truePtr = tmpPtr

	return blobs
}

func assignment(name string) {
	tmpPtr := truePtr
	buff := ""
	tempBuff := [][]string{}
	for {
		firstTerm, secondTerm := "", ""

		for tmpPtr < len(rawContents) && (rawContents[tmpPtr] != ' ' || rawContents[tmpPtr] == '=') {
			buff += string(rawContents[tmpPtr])
			tmpPtr++
		}
		firstTerm = buff
		buff = ""

		if rawContents[tmpPtr] == ' ' {
			tmpPtr++
		}
		tmpPtr += 2
		for tmpPtr < len(rawContents) && (rawContents[tmpPtr] != ' ' || rawContents[tmpPtr] == ',') {
			buff += string(rawContents[tmpPtr])
			tmpPtr++
		}
		secondTerm = buff

		key, value := strings.ReplaceAll(firstTerm, " ", ""), strings.ReplaceAll(secondTerm, " ", "")

		tmp := []string{key, value}
		tempBuff = append(tempBuff, tmp)

		truePtr = tmpPtr

		if tmpPtr >= len(rawContents) || rawContents[tmpPtr] == ' ' {
			break
		} else {
			truePtr += 2
		}

	}

	addToTagBuffer(name, tempBuff)
}

func predicate() {
	field := currToken
	nextToken(false)
	comparator := currToken
	nextToken(false)
	target := currToken

	if target[0] == '\'' {
		target = target[1:]
		iterPtr++
		for currToken[len(currToken)-1] != '\'' {
			target += " " + currToken
		}

		target += " " + currToken[:len(currToken)-1]
	}

	_predicate := shared.PredicatePayload{Field: field, Comparator: comparator, Target: target}
	addToTagBuffer("predicate", _predicate)

	nextToken(false)
	accept()
}

func addToTokenBuffer(str string) {
	tokenBuffer = append(tokenBuffer, str)
}

func parseFromTokenBuffer() shared.Instruction {
	inst := shared.Instruction{
		Operation: tokenBuffer[0],
	}

	for _, tag := range tagBuffer {
		inst.AddTag(tag)
	}
	tagBuffer = []shared.Tag{}
	tokenBuffer = []string{}

	return inst
}

func fatal(msgs ...string) {
	str := fmt.Sprint(msgs)

	panic(errors.New(str))
}

func contains(ls []string, tr string) bool {
	for _, lsTr := range ls {
		if tr == lsTr {
			return true
		}
	}
	return false
}

func addToTagBuffer(key string, value interface{}) {
	tagBuffer = append(tagBuffer, shared.Tag{
		Key:   key,
		Value: value,
	})
}
