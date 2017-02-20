package main

import (
	"github.com/Financial-Times/up-restutil/restutil"
	log "github.com/Sirupsen/logrus"
	"github.com/jawher/mow.cli"
	"golang.org/x/net/proxy"
	"os"
)

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
				restutil.Transport.Dial = dialer.Dial
			}
			if err := restutil.PutAllRest(*baseURL, *idProp, *user, *pass, *concurrency, *dumpFailed); err != nil {
				log.Fatal(err)
			}
		}

	})

	app.Command("put-binary-resources", "Read IDS from one endpoint and PUT them to another endpoint", func(cmd *cli.Cmd) {
		user := cmd.StringOpt("user", "", "user for basic auth")
		pass := cmd.StringOpt("pass", "", "password for basic auth")
		dumpFailed := cmd.BoolOpt("dump-failed", false, "dump failed resources to stdout, instead of exiting on failure")
		concurrency := cmd.IntOpt("concurrency", 16, "number of concurrent requests to use")
		throttle := cmd.IntOpt("throttle", 0, "number of PUT requests to make a second")
		fromBaseURL := cmd.StringArg("FROM_BASEURL", "", "base URL to PUT resources to")
		toBaseURL := cmd.StringArg("TO_BASEURL", "", "base URL to PUT resources to")
		cmd.Action = func() {
			if *socksProxy != "" {
				dialer, _ := proxy.SOCKS5("tcp", *socksProxy, nil, proxy.Direct)
				restutil.Transport.Dial = dialer.Dial
			}
			if err := restutil.PutAllBinaryRest(*fromBaseURL, *toBaseURL, *user, *pass, *concurrency, *throttle, *dumpFailed); err != nil {
				log.Fatal(err)
			}
		}
	})

	app.Command("dump-resources", "Read JSON resources from an endpoint and dump them to stdout", func(cmd *cli.Cmd) {
		baseURL := cmd.StringArg("BASEURL", "", "base URL to GET resources from. Must contain a __ids resource")
		throttle := cmd.IntOpt("throttle", 10, "Limit request rate for resource GET requests (requests per second)")
		cmd.Action = func() {
			if err := restutil.GetAllRest(*baseURL, *throttle); err != nil {
				log.Fatal(err)
			}
		}
	})

	app.Command("diff-ids", "Show differences between the ids available in two RESTful collections", func(cmd *cli.Cmd) {
		sourceURL := cmd.StringArg("SOURCEURL", "", "base URL to GET resources from. Must contain a __ids resource")
		destURL := cmd.StringArg("DESTURL", "", "base URL to GET resources from. Must contain a __ids resource")
		cmd.Action = func() {
			if err := restutil.DiffIDs(*sourceURL, *destURL); err != nil {
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
			service := &restutil.SyncService{
				DestIDsRetriever:   restutil.GetIDListRetriever(*destFile, *destURL),
				SourceIDsRetriever: restutil.GetIDListRetriever(*sourceFile, *sourceURL),
				Deletes:            *deletes,
				MaxConcurrentReqs:  *concurrency,
				MinExecTime:        *minExecTime,
				DestURL:            *destURL,
				SourceURL:          *sourceURL,
				Retries:            *retries,
			}
			if err := restutil.SyncIDs(service); err != nil {
				log.Fatal(err)
			}
		}
	})

	app.Run(os.Args)
}
