package jdbql

type TokenType int

const (
	TOKEN_TYPE_SYM TokenType = iota
	TOKEN_TYPE_SEMICOLON
	TOKEN_TYPE_QUOTE
	TOKEN_TYPE_TIC
	TOKEN_TYPE_BINOP
	TOKEN_TYPE_COMPARATOR
	TOKEN_TYPE_ASSIGNEMENT
	TOKEN_TYPE_LPAREN
	TOKEN_TYPE_RPAREN
	TOKEN_TYPE_COMMA
)

type JdbToken struct {
	contents []byte
	TokenType
}

func (token JdbToken) GetContents() []byte {
	return token.contents
}
