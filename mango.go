package cdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

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

func MangoFind(couchURL url.URL, dbName string, q MangoQuery, out interface{}) (err error) {
	var b []byte
	couchURL.Path = fmt.Sprintf("/%s/_find", dbName)
	if b, err = json.Marshal(q); err != nil {
		return err
	}
	buf := bytes.NewBuffer(b)
	log.Println(couchURL.String())
	log.Println(buf.String())
	cl, _ := http.Post(couchURL.String(), "application/json", buf)

	defer cl.Body.Close()

	b, _ = ioutil.ReadAll(cl.Body)
	json.Unmarshal(b, out)

	return nil
}
