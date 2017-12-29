package cdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

const SortAsc = "asc"
const SortDesc = "desc"

type MangoQuery struct {
	Selector map[string]MangoCondition `json:"selector"`
	Sort     []MangoSort               `json:"sort,omitempty"`
}

type MangoCondition struct {
	Eq  interface{} `json:"$eq,omitempty"`
	Ne  interface{} `json:"$ne,omitempty"`
	Gt  interface{} `json:"$gt,omitempty"`
	Lt  interface{} `json:"$lt,omitempty"`
	Gte interface{} `json:"$gte,omitempty"`
	Lte interface{} `json:"$lte,omitempty"`

	In  []interface{} `json:"$in,omitempty"`
	Nin []interface{} `json:"$nin,omitempty"`

	ElemMatch map[string]*MangoCondition `json:"$elemMatch,omitempty"`
}

type MangoSort map[string]string

type CouchIndex struct {
	IndexType  string               `json:"type"`
	Definition CouchIndexDefinition `json:"index"`
}

type CouchIndexDefinition struct {
	Fields []map[string]string `json:"fields"`
}

func MangoFind(couchURL url.URL, dbName string, q MangoQuery, out interface{}) (err error) {
	var b []byte
	couchURL.Path = fmt.Sprintf("/%s/_find", dbName)
	if b, err = json.Marshal(q); err != nil {
		return err
	}
	buf := bytes.NewBuffer(b)

	cl, err := http.Post(couchURL.String(), "application/json", buf)

	if err != nil {
		return err
	}

	defer cl.Body.Close()

	b, _ = ioutil.ReadAll(cl.Body)
	json.Unmarshal(b, out)

	return nil
}

func EnsureIndex(couchURL url.URL, dbName string, index CouchIndex) (err error) {
	var b []byte
	couchURL.Path = fmt.Sprintf("/%s/_index", dbName)
	if b, err = json.Marshal(index); err != nil {
		return err
	}
	buf := bytes.NewBuffer(b)

	_, err = http.Post(couchURL.String(), "application/json", buf)

	if err != nil {
		return err
	}

	return nil
}
