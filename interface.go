package crateio

import "fmt"

const (
	SQL_PATH = "/_sql"
)

type Query struct {
	Stmt     string          `json:"stmt"`
	Args     []interface{}   `json:"args,omitempty"`
	BulkArgs [][]interface{} `json:"bulk_args,omitempty"`
}

type Conn struct {
	servers []string
}

type Result struct {
	Cols     []string        `json:"cols"`
	ColTypes []interface{}   `json:"col_types"`
	Rows     [][]interface{} `json:"rows"`
	RowCount int32           `json:"rowcount"`
	Duration int             `json:"duration"`
	Results  []Result        `json:"results,omitempty"`
}

type SqlError struct {
	Detail *ErrorMessage `json:"error"`
	Trace  string        `json:"error_trace"`
}

type ErrorMessage struct {
	Message string `json:"message"`
	Code    uint   `json:"code"`
}

func (this *SqlError) Error() string {
	return fmt.Sprintf("Code:%d, Msg:%s", this.Detail.Code, this.Detail.Message)
}
