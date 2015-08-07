# logspout-kinesis

A [Logspout](https://github.com/gliderlabs/logspout) adapter for writing Docker container logs to [Amazon Kinesis](http://aws.amazon.com/kinesis/).

## usage 
After you created your custom Logspout build (see below), you can just run it as:
```console
$ 	docker run --rm \
		-e LOGSPOUT=ignore \
		--name="logspout" \
		--volume=/var/run/docker.sock:/var/run/docker.sock \
		mycompany/logspout kinesis://
```

You will need the Amazon credentials in the form of environment variables, or whatever suits you.
You will also neeed to set the environment variables `KINESIS_STREAM_TEMPLATE` and `KINESIS_PARTITION_KEY_TEMPLATE`, see below.

### streams and partition key configuration
You can decide to route your logs to one or multiple Kinesis Streams (and their names) via the the `KINESIS_STREAM_TEMPLATE` environment variable.  Golang's [text/template](http://golang.org/pkg/text/template/) package is used for templating, where the model available for templating is the [Message struct](https://github.com/gliderlabs/logspout/blob/master/router/types.go).

For instance, you can decide to assign a Kinesis stream for each of you app (assuming you have a `app` label on your running containers:
```console
$ export KINESIS_STREAM_TEMPLATE={{ index .Container.Config.Labels "app" }}
```

Or you can use environment variables, with our provided lookUp template function:
```console
$ export KINESIS_STREAM_TEMPLATE={{ lookUp .Container.Config.Env "APP_ID" }}
```

This will search through `.Container.Config.Env` for `APP_ID=*` and return the value after the `=`.

You can similarly decide the format of your partition key for each of your stream (here the app name and process type):
```console
$ export KINESIS_PARTITION_KEY_TEMPLATE={{ index .Container.Config.Labels "app" }}.{{ index .Container.Config.Labels "process.type" }}
```

## build
**logspout-kinesis** is a custom logspout module. To use it, create an empty Dockerfile based on `gliderlabs/logspout:master`, and import this `logspout-kinesis` package into a new `modules.go` file. The `gliderlabs/logspout` base image will `ONBUILD COPY` and replace the original `modules.go`. 

The following example creates a minimal logspout image capable of writing Dockers logs to Kinesis:

In modules.go:
```golang
package main

import (
	_ "github.com/gliderlabs/logspout/httpstream"
	_ "github.com/gliderlabs/logspout/routesapi"
	_ "github.com/remind101/logspout-kinesis"
)
```

In Dockerfile:
```Dockerfile
FROM gliderlabs/logspout:master
```

Final step, build the image:
```console
$ docker build -t mycompany/logspout .
```

More information on custom modules is available at the Logspout repo: [Custom Logspout Modules](https://github.com/gliderlabs/logspout/blob/master/custom/README.md)
