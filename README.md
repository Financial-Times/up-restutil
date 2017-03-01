## up-restutil

[![Circle CI](https://circleci.com/gh/Financial-Times/up-restutil/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/up-restutil/tree/master)

A utility for performing various (typically batched) RESTful operations with JSON resources.

# Installation
```
go get github.com/Financial-Times/up-restutil
```

# The 'put-resources' sub-command

PUTs all resources, reading from stdin, to a RESTful collection. For example to put JSON documents into http://localhost/foo :
```
echo '{"uuid":"63b76d37-bdce-4774-b9ac-8629c32ead7e"}{"uuid":"d122d243-4e04-4f4f-b935-ed8102872e50"}' | up-restutil put-resources uuid http://localhost/foo/
```
# The 'dump-resources' sub-command
GETs all resources from a RESTful collection. This expects a __ids resource that lists the identities of the resources in the form '{"id":"abc"}{"id":"123"}'

To manage the load on the endpoint, the number of GET requests per second can be limited.

```
up-restutil dump-resources --throttle=20 http://localhost/foo/
```

# The 'diff-ids' sub-command
Shows the differences between existence of resources in two collections using their __ids endpoints.

```
up-restutil diff-ids http://localhost/foo/ http://localhost/bar/
```

 Output is in the form :
```
{
  "only-in-source": [
    "a0233405-4a7f-3fea-9c9e-7681eb714a00",
    "760c7ddf-59d0-3ddc-aa15-7d410d5133b3"
  ],
  "only-in-destination": [
    "79eb0533-27e3-3282-9cac-e8ee083f7a9d"
  ]
}

```

# The 'sync-ids' sub-command
Creates or deletes resources in destination collection based on differences from source collection.  The content is not compared, only the existence.  An __ids endpoint is required.

```
up-restutil sync-ids http://localhost/foo/ http://localhost/bar/
```
Progress is shown during sync.  By default, deletion is not enabled in the destination during syncing, only creation. To enable delete, use --deletes=true 

# The 'put-binary-resources' sub-command
This behaves like the `concept-publisher`, it gets a list of IDs from one endpoint. It will then make a request for each ID and will then make a `PUT` request with the content of the body to another endpoint. An `__ids` endpoint is required from the "from" endpoint. It will then `PUT` the request at `toBaseURL/<UUID>`. This command does not care about the body content. It will just make a PUT request without parsing the body.

```
up-restutil put-binary-resources --user=username --pass=password --dump-failed=true --concurrency=10 --throttle=20 http://localhost/from/ http://localhost/to/
```
