use bb8_redis::{bb8::Pool, RedisConnectionManager};
use futures::{stream::TryStreamExt, StreamExt};
use redis::{AsyncCommands, RedisResult};
use std::hash::Hasher;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

const DEFAULT_REDIS_CONNECTION: u32 = 30;
const DEFAULT_CONCURRENCY: usize = 200;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut opts = getopts::Options::new();
    opts.reqopt("h", "host", "Redis host: redis://127.0.0.1/", "HOST");
    opts.reqopt("o", "output", "Output log file", "OUTPUT");
    opts.optopt(
        "",
        "redis_connection",
        &format!("redis connection number {DEFAULT_REDIS_CONNECTION}"),
        "NUM",
    );
    opts.optopt(
        "",
        "c_redis",
        &format!("concurrent number for redis: default {DEFAULT_CONCURRENCY}"),
        "NUM",
    );
    opts.optopt(
        "",
        "c_file",
        &format!("concurrent number for file: default {DEFAULT_CONCURRENCY}"),
        "NUM",
    );
    let matches = opts
        .parse(&args[1..])
        .expect(&opts.usage(&format!("Usage: {}", &args[0])));
    let redis_url = matches.opt_str("h").unwrap();
    let log_path = matches.opt_str("o").unwrap();
    let redis_connection = matches
        .opt_get_default::<u32>("redis_connection", DEFAULT_REDIS_CONNECTION)
        .unwrap();
    let concurency_redis = matches
        .opt_get_default::<usize>("c_redis", DEFAULT_CONCURRENCY)
        .unwrap();
    let concurency_file = matches
        .opt_get_default::<usize>("c_file", DEFAULT_CONCURRENCY)
        .unwrap();

    // reset log file
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&log_path)
        .await
        .unwrap();

    println!("Start... version: {:}", env!("CARGO_PKG_VERSION"));

    let manager = RedisConnectionManager::new(redis_url).unwrap();
    let pool = Pool::builder()
        .max_size(redis_connection)
        .build(manager)
        .await
        .unwrap();

    check_all(pool.clone(), &log_path, concurency_redis, concurency_file).await;
}

async fn check_all(
    pool: Pool<RedisConnectionManager>,
    log_path: &str,
    concurrency_redis: usize,
    concurrency_file: usize,
) {
    let keys = get_keys(pool.clone()).await.unwrap();
    check_values(
        pool.clone(),
        &log_path,
        concurrency_redis,
        concurrency_file,
        keys,
    )
    .await
    .unwrap();
}

async fn get_keys(pool: Pool<RedisConnectionManager>) -> RedisResult<Vec<String>> {
    println!("Start getting all keys");
    let mut con = pool.get().await.unwrap();
    let keys = con.keys("*").await?;
    Ok(keys)
}

async fn check_values(
    pool: Pool<RedisConnectionManager>,
    log_path: &str,
    concurrency_redis: usize,
    concurrency_file: usize,
    keys: Vec<String>,
) -> RedisResult<()> {
    println!("Start checking all values");
    futures::stream::iter(keys)
        .map(|key| check(pool.clone(), key))
        .buffered(concurrency_redis)
        .map(|x| x.unwrap())
        .map(|(key, hash_name, hash)| log(log_path, key, hash_name, hash))
        .buffered(concurrency_file)
        .try_collect::<()>()
        .await?;
    Ok(())
}

async fn check(
    pool: Pool<RedisConnectionManager>,
    key: String,
) -> RedisResult<(String, String, u64)> {
    let mut con = pool.get().await.unwrap();
    let type_name: String = redis::cmd("TYPE").arg(&key).query_async(&mut *con).await?;
    let result: Vec<String> = match &*type_name {
        "string" => vec![con.get(&key).await?],
        "set" => {
            let mut result: Vec<String> = con.smembers(&key).await?;
            result.sort();
            result
        }
        "list" => con.lrange(&key, 0, -1).await?,
        "hash" => {
            let result: Vec<String> = con.hgetall(&key).await?;
            let mut tuples: Vec<(&String, &String)> =
                result.chunks(2).map(|c| (&c[0], &c[1])).collect();
            tuples.sort_by(|a, b| a.0.cmp(b.0));
            tuples
                .iter()
                .flat_map(|t| vec![t.0.to_string(), t.1.to_string()])
                .collect()
        }
        "zset" => con.zrange(&key, 0, -1).await?,
        "none" => vec!["".to_string()],
        unknown => panic!("Not supported type '{unknown}' for key: {key}"),
    };
    let hash = calculate_hash(&result);

    Ok((key, type_name, hash))
}

async fn log(log_path: &str, key: String, type_name: String, hash: u64) -> RedisResult<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await?;
    let line = format!("{key} {type_name}: {hash:x}\n");
    file.write_all(&line.into_bytes()).await?;
    Ok(())
}

fn calculate_hash<T: std::hash::Hash>(t: &T) -> u64 {
    let mut s = std::collections::hash_map::DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
