## up-restutil

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


# More examples:

Copy all resources from http://localhost/foo/ to http://localhost/bar/ limiting the reads to 20 requests per second
```
up-restutil dump-resources --throttle=20 http://localhost/foo/ | up-restutil put-resources uuid http://localhost/foo/
```

