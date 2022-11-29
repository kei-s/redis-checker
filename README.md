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

- `-h`, `--host HOST`: Redis host: redis://127.0.0.1/
- `-o`, `--output OUTPUT`: Output log file
- `-c`, `--concurrency NUM`: concurrent number: default 50
