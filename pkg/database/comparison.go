package database

import (
	"fmt"
	"log"
	"strconv"
)

var (
	comparators []string = []string{">", "<", "=", "!=", "<=", ">="}
)

func compare(value, predicateTarget, targetType interface{}, comparator string) bool {
	log.Println(targetType)
	switch targetType.(string) {
	case "int":
		val1, val2 := tryParseInt(value), tryParseInt(predicateTarget)
		return compareInts(val1, val2, comparator)
	case "bool":
		return compareBools(value.(bool), predicateTarget.(bool), comparator)
	case "string":
		return compareStrings(value.(string), predicateTarget.(string), comparator)
	case "float":
		val1, val2 := tryParseFloat(value), tryParseFloat(predicateTarget)
		return compareFloats(val1, val2, comparator)
	case "char":
		return compareChars(value.(byte), predicateTarget.(byte), comparator)
	}

	return false
}

func compareInts(value, otherValue int, comparator string) bool {
	switch comparator {
	case ">":
		return value > otherValue
	case "<":
		return value < otherValue
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	case "<=":
		return value <= otherValue
	case ">=":
		return value >= otherValue
	}
	return false
}

func compareBools(value, otherValue bool, comparator string) bool {
	switch comparator {
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	}
	return false
}

func compareStrings(value, otherValue string, comparator string) bool {
	switch comparator {
	case ">":
		return value > otherValue
	case "<":
		return value < otherValue
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	case "<=":
		return value <= otherValue
	case ">=":
		return value >= otherValue
	}
	return false
}

func compareFloats(value, otherValue float64, comparator string) bool {
	switch comparator {
	case ">":
		return value > otherValue
	case "<":
		return value < otherValue
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	case "<=":
		return value <= otherValue
	case ">=":
		return value >= otherValue
	}
	return false
}

func compareChars(value, otherValue byte, comparator string) bool {
	switch comparator {
	case ">":
		return value > otherValue
	case "<":
		return value < otherValue
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	case "<=":
		return value <= otherValue
	case ">=":
		return value >= otherValue
	}
	return false
}

func tryParseInt(value interface{}) int {
	str := fmt.Sprint(value)

	val, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}

	return val
}

func tryParseFloat(value interface{}) float64 {
	str := fmt.Sprint(value)

	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		panic(err)
	}

	return val
}
