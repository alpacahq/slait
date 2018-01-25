# Slait

Slait is a simple buffered message queue and delivers messages at least once.


## Data Model

Slait aims to solve specific problems around shared time oriented data and made some design decisions for this purpose.

- All data consists of topics. A topic is a category of the same data flow.
- A topic consists of partitions. A partition within a topic is a single time-ordered stream.
- A partition name is a string unlike Kafka and partition allocation is dynamic.
- Clients can request the latest messages through the REST API as well as subscribe to the updates through the Websocket interface.
- Data is persisted on disk and stays in memory for fast access. The server restart will not cause any data loss.
- Data is retained for up to 5 days by default. The custom retention policy will come in the future.
- Most topic and partition operations can be done through the REST API online.
- For more details on the persistency layer, see commitlog/doc.go


## Configuration

The configuration parameters are as follows in the YAML format and you can pass it via `-config` option to the command line.

- ListenPort: the port number string.  It will bind to all the available interfaces on this port.
- LogLevel: one of the ERROR, WARNING, or INFO
- DataDir: the root base directory to put the persistent data.


## API specification

See documentation/rest.md


## Build

Slait requires Go 1.9+.

`make configure` is needed to download all the dependencies.
`make all` will build the binary and install from `./cmd` directory to `$GOPATH/bin`.



