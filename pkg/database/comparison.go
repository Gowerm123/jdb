package database

var (
	comparators []string = []string{">", "<", "=", "!=", "<=", ">="}
)

func compare(value, predicateTarget interface{}, targetType string, comparator string) bool {
	switch targetType {
	case "int":
		return compareInts(value.(int), predicateTarget.(int), comparator)
	case "bool":
		return compareBools(value.(bool), predicateTarget.(bool), comparator)
	case "string":
		return compareStrings(value.(string), predicateTarget.(string), comparator)
	case "float":
		return compareFloats(value.(float64), predicateTarget.(float64), comparator)
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
