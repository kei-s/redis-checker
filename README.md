# Redis checker

## Usage

```
$ redis-checker -h redis:://127.0.0.1/ -o redis-values.log
```

## Result

```
{key} {type}: {hash}
```

e.g. `$ cat redis-values.log`

```
hash_key hash: 1bfe4fff767ce910
ruby:queues set: 14ef480f02df7b32
ruby:processes set: f12aade4ebe04f04
```

## Options

```
Options:
    -h, --host HOST     Redis host: redis://127.0.0.1/
    -o, --output OUTPUT Output log file
        --redis_connection NUM
                        redis connection number 30
        --c_redis NUM   concurrent number for redis: default 200
        --c_file NUM    concurrent number for file: default 200
```
