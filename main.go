package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/jawher/mow.cli"
	"golang.org/x/net/proxy"
	"gopkg.in/cheggaaa/pb.v1"
)

const useragent = "up-restutil"

func main() {

	app := cli.App("up-restutil", "A RESTful resource utility")

	socksProxy := app.StringOpt("socks-proxy", "", "Use specified SOCKS proxy (e.g. localhost:2323)")

	app.Command("put-resources", "read json resources from stdin and PUT them to an endpoint", func(cmd *cli.Cmd) {
		user := cmd.StringOpt("user", "", "user for basic auth")
		pass := cmd.StringOpt("pass", "", "password for basic auth")
		concurrency := cmd.IntOpt("concurrency", 16, "number of concurrent requests to use")
		idProp := cmd.StringArg("IDPROP", "", "property name of identity property")
		baseUrl := cmd.StringArg("BASEURL", "", "base URL to PUT resources to")
		cmd.Action = func() {
			if *socksProxy != "" {
				dialer, _ := proxy.SOCKS5("tcp", *socksProxy, nil, proxy.Direct)
				transport.Dial = dialer.Dial
			}
			if err := putAllRest(*baseUrl, *idProp, *user, *pass, *concurrency); err != nil {
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

	app.Command("diff-ids", "show differences between the ids available in two RESTful collections", func(cmd *cli.Cmd) {
		sourceUrl := cmd.StringArg("SOURCEURL", "", "base URL to GET resources from. Must contain a __ids resource")
		destUrl := cmd.StringArg("DESTURL", "", "base URL to GET resources from. Must contain a __ids resource")
		cmd.Action = func() {
			if err := diffIDs(*sourceUrl, *destUrl); err != nil {
				log.Fatal(err)
			}
		}
	})

	app.Command("sync-ids", "show differences between the ids available in two RESTful collections", func(cmd *cli.Cmd) {
		deletes := cmd.BoolOpt("deletes", false, "delete from destination those resources not present in source")
		sourceUrl := cmd.StringArg("SOURCEURL", "", "base URL to GET resources from. Must contain a __ids resource")
		destUrl := cmd.StringArg("DESTURL", "", "base URL to GET resources from. Must contain a __ids resource")
		cmd.Action = func() {
			if err := syncIDs(*sourceUrl, *destUrl, *deletes); err != nil {
				log.Fatal(err)
			}
		}
	})

	app.Run(os.Args)
}

func putAllRest(baseurl string, idProperty string, user string, pass string, conns int) error {

	dec := json.NewDecoder(os.Stdin)

	docs := make(chan resource)

	transport.MaxIdleConnsPerHost = conns

	rp := &resourcePutter{baseurl, idProperty, user, pass}

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

func diffIDs(sourceURL, destURL string) error {
	sourceIDs := make(chan string)
	go fetchIDList(sourceURL, sourceIDs)

	destIDs := make(chan string)
	go fetchIDList(destURL, destIDs)

	sources := make(map[string]struct{})
	dests := make(map[string]struct{})

	for sourceIDs != nil || destIDs != nil {
		select {
		case sourceID, ok := <-sourceIDs:
			if !ok {
				sourceIDs = nil
			} else {
				sources[sourceID] = struct{}{}
			}
		case destID, ok := <-destIDs:
			if !ok {
				destIDs = nil
			} else {
				dests[destID] = struct{}{}
			}
		}
	}

	var output struct {
		OnlyInSource      []string `json:"only-in-source"`
		OnlyInDestination []string `json:"only-in-destination"`
	}

	for s, _ := range sources {
		if _, found := dests[s]; !found {
			output.OnlyInSource = append(output.OnlyInSource, s)
		} else {
			delete(dests, s)
		}

	}

	for s, _ := range dests {
		output.OnlyInDestination = append(output.OnlyInDestination, s)
	}

	return json.NewEncoder(os.Stdout).Encode(output)

}

func syncIDs(sourceURL, destURL string, deletes bool) error {
	sourceIDs := make(chan string)
	go fetchIDList(sourceURL, sourceIDs)

	destIDs := make(chan string)
	go fetchIDList(destURL, destIDs)

	sources := make(map[string]struct{})
	dests := make(map[string]struct{})

	for sourceIDs != nil || destIDs != nil {
		select {
		case sourceID, ok := <-sourceIDs:
			if !ok {
				sourceIDs = nil
			} else {
				sources[sourceID] = struct{}{}
			}
		case destID, ok := <-destIDs:
			if !ok {
				destIDs = nil
			} else {
				dests[destID] = struct{}{}
			}
		}
	}

	var output struct {
		Created int `json:"created"`
		Deleted int `json:"created"`
	}

	sem := make(chan struct{}, 64)
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	errs := make(chan error, 1)

	if len(sources) > 0 {
		bar := pb.StartNew(len(sources))

		for s, _ := range sources {
			if _, found := dests[s]; !found {
				select {
				case err := <-errs:
					return err
				default:
					<-sem
					go func() {
						defer func() { sem <- struct{}{} }()
						if err := doCopy(sourceURL, destURL, s); err != nil {
							errs <- err
						}
					}()
					output.Created++
					bar.Increment()
				}
			} else {
				delete(dests, s)
			}

		}
		bar.FinishPrint("Done creates")
	}

	if deletes && len(dests) > 0 {
		bar := pb.StartNew(len(dests))

		for s, _ := range dests {
			if err := doDelete(destURL, s); err != nil {
				return err
			}
			output.Deleted++
			bar.Increment()
		}
		bar.FinishPrint("Done deletes")
	}

	return json.NewEncoder(os.Stdout).Encode(output)

}

func doCopy(sourceURL, destURL, id string) error {

	su := sourceURL
	if !strings.HasSuffix(su, "/") {
		su = su + "/"
	}

	sreq, err := http.NewRequest("GET", fmt.Sprintf("%s%s", su, id), nil)
	if err != nil {
		return err
	}
	sreq.Header.Set("User-Agent", useragent)
	sresp, err := httpClient.Do(sreq)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, sresp.Body)
		_ = sresp.Body.Close()
	}()

	if sresp.StatusCode != http.StatusOK {
		return fmt.Errorf("error copying resource: %s", sresp.Status)
	}

	du := destURL
	if !strings.HasSuffix(du, "/") {
		du = du + "/"
	}

	dreq, err := http.NewRequest("PUT", fmt.Sprintf("%s%s", du, id), sresp.Body)
	if err != nil {
		return err
	}
	dreq.Header.Set("User-Agent", useragent)
	dresp, err := httpClient.Do(dreq)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, dresp.Body)
		_ = dresp.Body.Close()
	}()
	if dresp.StatusCode != http.StatusOK {
		return fmt.Errorf("error copying resource: %s", dresp.Status)
	}

	return nil
}

func doDelete(destURL, id string) error {

	du := destURL
	if !strings.HasSuffix(du, "/") {
		du = du + "/"
	}

	dreq, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s", du, id), nil)
	if err != nil {
		return err
	}
	dresp, err := httpClient.Do(dreq)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, dresp.Body)
		_ = dresp.Body.Close()
	}()
	if dresp.StatusCode != http.StatusOK {
		return fmt.Errorf("error deleting resource: %s", dresp.Status)
	}
	return nil
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
		req.Header.Set("User-Agent", useragent)

		if rp.user != "" && rp.pass != "" {
			req.SetBasicAuth(rp.user, rp.pass)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode > 299 {
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

	transport.MaxIdleConnsPerHost = readers

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

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("User-Agent", useragent)
	resp, err := httpClient.Do(req)
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
		req, err := http.NewRequest("GET", strings.Join([]string{baseURL, id}, ""), nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("User-Agent", useragent)
		resp, err := httpClient.Do(req)
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

var (
	transport = &http.Transport{
		MaxIdleConnsPerHost: 128,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	}

	httpClient = &http.Client{
		Transport: transport,
	}
)

type resource map[string]interface{}

type resourcePutter struct {
	baseUrl    string
	idProperty string
	user       string
	pass       string
}
