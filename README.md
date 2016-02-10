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

# More examples:

Copy all resources from http://localhost/foo/ to http://localhost/bar/ limiting the reads to 20 requests per second
```
up-restutil dump-resources --throttle=20 http://localhost/foo/ | up-restutil put-resources uuid http://localhost/foo/
```

