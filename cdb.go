package cdb

import (
	"net/http"
	"net/url"

	couchdb "github.com/fjl/go-couchdb"
)

var conns map[string]*couchdb.Client

func init() {
	conns = make(map[string]*couchdb.Client)
}

func GetConnection(connectionURL url.URL) (c *couchdb.Client, err error) {
	urls := connectionURL.String()
	if c, ok := conns[urls]; ok {
		return c, nil
	}

	c, err = couchdb.NewClient(urls, http.DefaultTransport)
	conns[urls] = c
	return c, nil
}
