package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jawher/mow.cli"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {

	app := cli.App("up-restutil", "A RESTful resource utility")

	app.Command("put-resources", "read json resources from stdin and PUT them to an endpoint", func(cmd *cli.Cmd) {
		idProp := cmd.StringArg("IDPROP", "", "property name of identity property")
		baseUrl := cmd.StringArg("BASEURL", "", "base URL to PUT resources to")
		cmd.Action = func() {
			if err := putAllRest(*baseUrl, *idProp, 1024); err != nil {
				log.Fatal(err)
			}
		}

	})

	app.Run(os.Args)
}

func putAllRest(baseurl string, idProperty string, conns int) error {

	dec := json.NewDecoder(os.Stdin)

	docs := make(chan resource)

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: conns,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
		},
	}

	rp := &resourcePutter{baseurl, idProperty, httpClient}

	errs := make(chan error, 1)

	wg := sync.WaitGroup{}
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			if err := rp.putAll(docs); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
			wg.Done()
		}()
	}

	for {
		var doc map[string]interface{}
		if err := dec.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		select {
		case docs <- doc:
		case err := <-errs:
			return err
		}
	}

	close(docs)

	wg.Wait()

	select {
	case err := <-errs:
		return err
	default:
		return nil
	}

}

func (rp *resourcePutter) putAll(resources <-chan resource) error {
	for r := range resources {
		id := r[rp.idProperty]
		idStr, ok := id.(string)
		if !ok {
			log.Printf("unable to extract id property from resource, skipping")
		}

		msg, err := json.Marshal(r)
		if err != nil {
			return err
		}
		b := rp.baseUrl
		if !strings.HasSuffix(b, "/") {
			b = b + "/"
		}
		u, err := url.Parse(b)
		if err != nil {
			return err
		}
		u, err = u.Parse(idStr)
		if err != nil {
			return err
		}
		req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(msg))
		if err != nil {
			return err
		}
		resp, err := rp.client.Do(req)
		if err != nil {
			return err
		}
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != 200 && resp.StatusCode != 202 {
			return fmt.Errorf("http fail: %v :\n%s\n", resp.Status, contents)
		}

	}
	return nil
}

type resource map[string]interface{}

type resourcePutter struct {
	baseUrl    string
	idProperty string
	client     *http.Client
}
