# Phase 2 Plan

## TODO
- Handle cases where configuration is changed and that results in making some of the 
  previously stored data illeagal.

## Improvements
### Functionality
- Add access token and signature to each request and validate the token with the signature.
  Self-contained-type access token together with public key encryption can be used 
  to avoid accessing remote server for validation.
- Add an unregister topic and unsubscribe topic APIs

### Code
#### Speed
- Locate the MQ server and Redis instances on the same host and use Unix domain socket.
- Use negative index to access the latter half of the list in lindex.
- Cache messages e.g. cache popular messages with LRU eviction.
- Avoid throwing exception if that can be replaced by error return value.
#### User experience
- Return a more descriptive value than a boolean value from API calls other than get.

### Robustness (addressing message lost issue)
- Use Redis cluster to gain redundancy to cover cases where a subset of the cluster fail.
- Turn on AOF persistence with fsync running every second, and add an API to gracefully 
  shutdown MQ server by not accepting API calls other than get for one second to let AOF fsync 
  unpersisted data and then shutdown.

### Scalability (covering 20000 rps and 1-second-response requirement)
- Add replicas of the master Redis instance so that the same message can be retrieved from 
  multiple slave Redis instances.
- Use sharding and distribute topics among different Redis clusters. e.g. distribute topics 
  by using first N bits of topic name hash where N is a number of bits required to represent 
  the number of shards.

