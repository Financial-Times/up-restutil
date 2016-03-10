package main

import (
	"bytes"
	"encoding/json"
	"errors"
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
			if err := putAllRest(*baseUrl, *idProp, 128); err != nil {
				log.Fatal(err)
			}
		}

	})

	app.Command("dump-resources", "read json resources from stdin and PUT them to an endpoint", func(cmd *cli.Cmd) {
		baseUrl := cmd.StringArg("BASEURL", "", "base URL to GET resources from. Must contain a __ids resource")
		throttle := cmd.IntOpt("throttle", 10, "Limit request rate for resource GET requests (requests per second)")
		cmd.Action = func() {
			if err := getAllRest(*baseUrl, *throttle); err != nil {
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
			log.Println("unable to extract id property from resource, skipping")
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

func getAllRest(baseURL string, throttle int) error {
	if baseURL == "" {
		return errors.New("baseURL must be provided")
	}
	if !strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL + "/"
	}
	if throttle < 1 {
		log.Fatalf("Invalid throttle %d", throttle)
	}
	ticker := time.NewTicker(time.Second / time.Duration(throttle))

	messages := make(chan string, 128)

	go func() {
		fetchAll(baseURL, messages, ticker)
		close(messages)
	}()

	for msg := range messages {
		fmt.Println(msg)
	}
	return nil
}

func fetchAll(baseURL string, messages chan<- string, ticker *time.Ticker) {
	ids := make(chan string, 128)
	go fetchIDList(baseURL, ids)

	readers := 32

	readWg := sync.WaitGroup{}

	for i := 0; i < readers; i++ {
		readWg.Add(1)
		go func(i int) {
			fetchMessages(baseURL, messages, ids, ticker)
			readWg.Done()
		}(i)
	}

	readWg.Wait()
}

func fetchIDList(baseURL string, ids chan<- string) {

	u, err := url.Parse(baseURL)
	if err != nil {
		log.Fatal(err)
	}
	u, err = u.Parse("./__ids")
	if err != nil {
		log.Fatal(err)
	}

	resp, err := httpClient.Get(u.String())
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	type listEntry struct {
		ID string `json:id`
	}

	var le listEntry
	dec := json.NewDecoder(resp.Body)
	for {
		err = dec.Decode(&le)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		ids <- le.ID
	}

	close(ids)
}

func fetchMessages(baseURL string, messages chan<- string, ids <-chan string, ticker *time.Ticker) {
	for id := range ids {
		<-ticker.C
		resp, err := httpClient.Get(strings.Join([]string{baseURL, id}, ""))
		if err != nil {
			panic(err)
		}
		data, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}
		messages <- string(data)
	}
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 32,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	},
}

type resource map[string]interface{}

type resourcePutter struct {
	baseUrl    string
	idProperty string
	client     *http.Client
}
