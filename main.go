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

	app.Command("put-resources", "Read JSON resources from stdin and PUT them to an endpoint", func(cmd *cli.Cmd) {
		user := cmd.StringOpt("user", "", "user for basic auth")
		pass := cmd.StringOpt("pass", "", "password for basic auth")
		dumpFailed := cmd.BoolOpt("dump-failed", false, "dump failed resources to stdout, instead of exiting on failure")
		concurrency := cmd.IntOpt("concurrency", 16, "number of concurrent requests to use")
		idProp := cmd.StringArg("IDPROP", "", "property name of identity property")
		baseURL := cmd.StringArg("BASEURL", "", "base URL to PUT resources to")
		cmd.Action = func() {
			if *socksProxy != "" {
				dialer, _ := proxy.SOCKS5("tcp", *socksProxy, nil, proxy.Direct)
				transport.Dial = dialer.Dial
			}
			if err := putAllRest(*baseURL, *idProp, *user, *pass, *concurrency, *dumpFailed); err != nil {
				log.Fatal(err)
			}
		}

	})

	app.Command("dump-resources", "Read JSON resources from an endpoint and dump them to stdout", func(cmd *cli.Cmd) {
		baseURL := cmd.StringArg("BASEURL", "", "base URL to GET resources from. Must contain a __ids resource")
		throttle := cmd.IntOpt("throttle", 10, "Limit request rate for resource GET requests (requests per second)")
		cmd.Action = func() {
			if err := getAllRest(*baseURL, *throttle); err != nil {
				log.Fatal(err)
			}
		}
	})

	app.Command("diff-ids", "Show differences between the ids available in two RESTful collections", func(cmd *cli.Cmd) {
		sourceURL := cmd.StringArg("SOURCEURL", "", "base URL to GET resources from. Must contain a __ids resource")
		destURL := cmd.StringArg("DESTURL", "", "base URL to GET resources from. Must contain a __ids resource")
		cmd.Action = func() {
			if err := diffIDs(*sourceURL, *destURL); err != nil {
				log.Fatal(err)
			}
		}
	})

	app.Command("sync-ids", "Sync resources between two RESTful JSON collections, using PUT and DELETE on the destination as needed", func(cmd *cli.Cmd) {
		deletes := cmd.BoolOpt("deletes", false, "delete from destination those resources not present in source")
		concurrency := cmd.IntOpt("concurrency", 32, "number of concurrent requests to use")
		minExecTime := cmd.IntOpt("minExecTime", 0, "minimum amount of seconds it will take to execute one sync operation")
		retries := cmd.IntOpt("retries", 2, "number of times a sync should be retried if it fails")
		sourceFile := cmd.StringOpt("sourceFile", "", "path to the file the contains the source ids")
		destFile := cmd.StringOpt("destFile", "", "path to the file that contains the destination ids")
		sourceURL := cmd.StringArg("SOURCEURL", "", "base URL to GET resources from. Must contain a __ids resource")
		destURL := cmd.StringArg("DESTURL", "", "base URL to GET resources from. Must contain a __ids resource")
		cmd.Action = func() {
			service := &syncService{
				destIDsRetriever:   getIDListRetriever(*destFile, *destURL),
				sourceIDsRetriever: getIDListRetriever(*sourceFile, *sourceURL),
				deletes:            *deletes,
				maxConcurrentReqs:  *concurrency,
				minExecTime:        *minExecTime,
				destURL:            *destURL,
				sourceURL:          *sourceURL,
				retries:            *retries,
			}
			if err := syncIDs(service); err != nil {
				log.Fatal(err)
			}
		}
	})

	app.Run(os.Args)
}

func putAllRest(baseURL string, idProperty string, user string, pass string, conns int, dumpFailed bool) error {

	dec := json.NewDecoder(os.Stdin)

	docs := make(chan resource)

	transport.MaxIdleConnsPerHost = conns

	rp := &resourcePutter{baseURL, idProperty, user, pass}

	errs := make(chan error, 1)

	var failChan chan []byte

	failwg := sync.WaitGroup{}

	if dumpFailed {
		failChan = make(chan []byte)

		failwg.Add(1)
		go func() {
			defer failwg.Done()

			for resource := range failChan {
				_, err := os.Stdout.Write(resource)
				if err == nil {
					_, err = io.WriteString(os.Stdout, "\n")
				}
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		}()
	}

	wg := sync.WaitGroup{}

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			if err := rp.putAll(docs, failChan); err != nil {
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

	if dumpFailed {
		close(failChan)
		failwg.Wait()
	}

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

	output.OnlyInSource = []string{}
	output.OnlyInDestination = []string{}

	for s := range sources {
		if _, found := dests[s]; !found {
			output.OnlyInSource = append(output.OnlyInSource, s)
		} else {
			delete(dests, s)
		}

	}

	for s := range dests {
		output.OnlyInDestination = append(output.OnlyInDestination, s)
	}

	return json.NewEncoder(os.Stdout).Encode(output)

}

type syncService struct {
	sourceIDsRetriever IDListRetriever
	destIDsRetriever   IDListRetriever
	sourceURL          string
	destURL            string
	maxConcurrentReqs  int
	minExecTime        int
	retries            int
	deletes            bool
}

func syncIDs(service *syncService) error {
	errChan := make(chan error)
	defer close(errChan)
	sourceIDs := make(chan string)
	go service.sourceIDsRetriever.Retrieve(sourceIDs, errChan)
	destIDs := make(chan string)
	go service.destIDsRetriever.Retrieve(destIDs, errChan)

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
		case err := <-errChan:
			return err
		}
	}

	var output struct {
		Created int `json:"created"`
		Deleted int `json:"created"`
	}

	sem := make(chan struct{}, service.maxConcurrentReqs)
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	errs := make(chan error, 1)

	if len(sources) > 0 {
		var wg sync.WaitGroup
		bar := pb.StartNew(len(sources))

		for s := range sources {
			if _, found := dests[s]; !found {
				select {
				case err := <-errs:
					return err
				default:
					<-sem
					wg.Add(1)
					go func(id string) {
						defer func() {
							sem <- struct{}{}
							wg.Done()
						}()
						minExecTime := time.After(time.Second * time.Duration(service.minExecTime))
						retry := service.retries
						for {
							if err := doCopy(service.sourceURL, service.destURL, id); err != nil {
								if retry == 0 {
									errs <- err
									break
								} else {
									retry--
									time.Sleep(time.Second * 2)
								}
							} else {
								break
							}
						}
						<-minExecTime
					}(s)
					output.Created++
					bar.Increment()
				}
			} else {
				bar.Increment()
				delete(dests, s)
			}

		}
		wg.Wait()
		bar.FinishPrint("Done creates")
	}

	if service.deletes && len(dests) > 0 {
		bar := pb.StartNew(len(dests))

		for s := range dests {
			if err := doDelete(service.destURL, s); err != nil {
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
	dreq.Header.Set("Content-type", "application/json")
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

func (rp *resourcePutter) putAll(resources <-chan resource, failChan chan []byte) error {
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
		b := rp.baseURL
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
		err = rp.put(u.String(), bytes.NewReader(msg))
		if err != nil {
			if failChan != nil {
				failChan <- msg
			} else {
				return err
			}
		}
	}
	return nil
}

func (rp *resourcePutter) put(url string, data io.Reader) error {
	req, err := http.NewRequest("PUT", url, data)
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
		ID string `json:"id"`
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
	baseURL    string
	idProperty string
	user       string
	pass       string
}
