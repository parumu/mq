# Message Queue
Message queue service with the following characteristics:
- Each topic has an associated fixed size queue. The queue evicts older messages to store messages in the fixed size queue.
- Topic/owner/subscriber name has a length limit
- There is a message size limit
- There is a limit for maximum number of topics
- There is a limit for maximum number of subscribers
 
## Configuration
- Files
  - src/main/resources/log4j.properties
  - src/main/resources/config.properties 
    - Configurable keys and the examples:
```
# address that Message queue service binds to
host = 0.0.0.0 

# port that Message queue service listens to
port = 8080

# Redis host and port
redisHost = localhost
redisPort = 6379

maxQueueSize = 10000
maxNameLen = 100

# 128KB = 128 * 1024 = 131072
maxMsgSize = 131072

maxTopics = 10000
maxSubscribers = 10000
```  
  
- Redis
  - Should set `maxmemory-policy: noeviction` to avoid Redis to evicts data from memory.

## Building a docker image
Run docker build command in the project root directory. e.g. 
```
$ cd [project root directory]
$ docker build -t mq .
```

## Running integration tests
- Integration tests require that Redis run locally. To set that up:
```
$ cd [project root directory]
$ docker-compose up
```

## API
| Endpoint | Request | Result | Description |
|---------|----------|-------|--------|
| v1/topic/register | "owner": string<br>"topic": string | <ul><li>200 success</li><li>400 error</li></ul> | Register a topic specifying the owner |
| v1/message/publish | "owner": string<br>"topic": string<br>"msg": string | <ul><li>200 success</li><li>400 error</li></ul> | Publish a message to a topic specifying the owner | 
| v1/topic/subscribe | "subscriber": string<br>"topic": string | <ul><li>200 success</li><li>400 error</li></ul> | Let a user subscribe to a topic |
| v1/message/get | "subscriber": string<br>"topic": string | <ul><li>200 success<ul><li>msgIdx: int<br>msg: string</li></ul></li><li>230 message evicted</li><li>231 no message</li><li>400 error</ul> | Let a user retrieve the latest unacked message in a topic |
| v1/message/ack | "subscriber": string<br>"topic": string<br>"msgIdx":number | <ul><li>200 success</li><li>400 error</li></ul> | Let a user ack a message of a topic |

## Data Structure
```
Ledends: <X>: actual value for X
--
num-topics -> # of topics

T-topic -> linked list of messages for topic T e.g. ["msg1", "msg2", .... "msgN"]

T-topic-attr
  - owner -> <topic owner>
  - msg-head -> index of the message at the beginnig of the topic list (message index is 1-based)
  - num-subscs -> number of subscribers

T-topic-subsc
  - <subscriber> -> next message index to get of the subscriber
```

- Register API call newly stores T-topic-attr/owner, T-topic/attr/msg-head=0 and 
  T-topic/attr/num-subscs=0 for the topic.
- Subscribe API call sets T-topic-subsc-<subscriber> to T-topic-attr/msg-head + 1
- Publish API call adds a message in front of T-topic list and increase T-topic/attr/msg-head by 1.
  If T-topic list is full, the message at the end is evicted before adding the new massage.
- Get API call attempts to get a message at T-topic-subsc-<subscriber>. If the message at the index 
  has not been published, the subscriber gets 231 no message. If the message at the index has been 
  evicted from the list, the subscriber gets 230 message evicted.
- Ack API call acks a message at T-topic-subsc-<subscriber>, and increases T-topic-subsc-<subscriber> by 1.

## Runtime complexity 
- Get: O(1) to O(n)   
  - Messages are stored in linked list.
    Getting message at the beggining and end is O(1) and O(n) respectively.
- Others: O(1)

## Maximum memory usage
- Maximum memory usage by the configuration can be rougnly calculated with the following formula:
```
lists = maxMsgSize * maxQueueSize * maxTopics
topics = maxNameLen * maxTopics
owners = maxNameLen * maxTopics
subscribers = subscriber * maxSubscribers * maxTopics

maxMemoryUsage = lists + topics + owners + subscribers
```


