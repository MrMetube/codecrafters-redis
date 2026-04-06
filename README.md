[![progress-banner](https://backend.codecrafters.io/progress/redis/7c2a321a-c400-483a-85d0-bef9d7fe5a5d)](https://app.codecrafters.io/users/MrMetube?r=2qF)

This is based on the ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

It is a toy Redis clone that's capable of handling
a subset of all commands:

#### Basics
- SET
- GET
- INCR

#### Meta
- PING
- ECHO
- TYPE
  
#### Lists  
- RPUSH
- LPUSH
- LLEN
- LRANGE
- LPOP
- BLPOP

#### Streams    
- XADD
- XRANGE
- XREAD
    
#### Transactions
- MULTI
- EXEC
- DISCARD
    
#### Sorted Set  
- ZADD
- ZREM
- ZRANK
- ZSCORE
- ZRANGE
- ZCARD