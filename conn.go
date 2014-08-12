package crateio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
)

func NewConn(servers []string) *Conn {
	return &Conn{servers: servers}
}

func (this *Conn) Query(stmt string, args ...interface{}) (*Result, error) {
	endpoint := fmt.Sprintf("http://%s%s?types&error_trace=True", this.selectServer(), SQL_PATH)
	query := &Query{
		Stmt: stmt,
		Args: args,
	}
	buf, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	data := bytes.NewReader(buf)

	resp, err := http.Post(endpoint, "application/json", data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	myError := &SqlError{}
	err = json.Unmarshal(body, myError)
	if err != nil {
		return nil, err
	}
	if myError.Detail != nil {
		return nil, myError
	}

	result := &Result{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (this *Conn) selectServer() string {
	pos := rand.Intn(len(this.servers))
	return this.servers[pos]
}
