## Slait REST API Specification

# /topics [GET]

* Description: Retrieve a list of topics.

* Input: None

* Output: JSON structured array of topic names.

* Example:

```
curl http://localhost:5995/topics

["quotes","bars"]
```

# /topics [POST]

* Description: Create a new topic.

* Input: JSON object defining topic name, and its underlying partitions

* Output: None

* Example:

```
curl -X POST -d '{"topic":"bars","partitions":["NVDA","AMD"]}' http://localhost:5995/topics
```


# /topics [DELETE]

* Description: Delete all topics.

* Input: None

* Output: None

* Example:

```
curl -X DELETE -d '{"topic":"bars"}' http://localhost:5995/topics
```


# /topics/{topic} [GET]

* Description: Query list of partitions within {topic}.

* Input: None

* Output: None

* Example:

```
curl http://127.0.0.1:5994/topics/bars

{"AMD","NVDA"}
```


# /topics/{topic} [DELETE]

* Description: Remove {topic} and all of its partitions.

* Input: None

* Output: None

* Example:

```
curl http://127.0.0.1:5994/topics/bars
```


# /topics/{topic}/{partition} [GET]

* Description: Query entries from {partition} within {topic}.

* Input: None

* Output: JSON structured array of stored data under {topic} and {partition}.

* Example:

```
curl http://127.0.0.1:5994/topics/bars

{"Data":[{"Timestamp":"2017-08-25T23:00:00Z","Data":"eyJzb21lIjoianNvbiIsImRhdGEiOiJoZXJlIn0="}]}
```


# /topics/{topic}/{partition} [PUT]

* Description: Append new entries to {partition} within {topic}. A new partition is made if {partition} does not already exist.

* Input: JSON structured array of data to be stored under {topic} and {partition}

* Output: None

* Example:

```
curl -X PUT -d '{"data":[{"timestamp":"2017-08-25T23:00:00Z", "data":{"some":"json","data":"here"}}]}' http://localhost:5995/topics/bars/AMD
```


# /topics/{topic}/{partition} [DELETE]

* Description: Delete {partition} from {topic} along with all of its entries.

* Input: None

* Output: None

* Example:

```
curl -X DELETE http://localhost:5995/topics/bars/AMD
```
