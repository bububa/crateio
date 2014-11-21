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
	endpoint := fmt.Sprintf("http://%s%s?types", this.selectServer(), SQL_PATH)
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

func (this *Conn) BulkQuery(stmt string, args [][]interface{}) (*Result, error) {
	endpoint := fmt.Sprintf("http://%s%s?types", this.selectServer(), SQL_PATH)
	query := &Query{
		Stmt:     stmt,
		BulkArgs: args,
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
	if len(this.servers) == 0 {
		return "localhost:4200"
	}
	if len(this.servers) == 1 {
		return this.servers[0]
	}
	pos := rand.Intn(len(this.servers))
	return this.servers[pos]
}
