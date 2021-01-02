Scenarios to be tested:


- start with empty database and fill with random keys
- read/write mixed ops, get operations per second, (total, only read, only write)


- configuration parameters
  * key length
  * value size
  * ratio of read/write

- things to measure
  * recover time (initial insert)
  * commit time (markcomplete)
  * read/write/both ops per second
  * memory usage
  * cpu usage (not done yet)