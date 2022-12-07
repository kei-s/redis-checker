use futures::{stream::TryStreamExt, StreamExt};
use redis::{AsyncCommands, RedisResult};
use std::hash::Hasher;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

const DEFAULT_CONCURRENCY: usize = 50;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut opts = getopts::Options::new();
    opts.reqopt("h", "host", "Redis host: redis://127.0.0.1/", "HOST");
    opts.reqopt("o", "output", "Output log file", "OUTPUT");
    opts.optopt(
        "c",
        "concurrency",
        &format!("concurrent number: default {DEFAULT_CONCURRENCY}"),
        "NUM",
    );
    let matches = opts
        .parse(&args[1..])
        .expect(&opts.usage(&format!("Usage: {}", &args[0])));
    let redis_url = matches.opt_str("h").unwrap();
    let log_path = matches.opt_str("o").unwrap();
    let concurency = matches
        .opt_get_default::<usize>("c", DEFAULT_CONCURRENCY)
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

    let keys = get_keys(&redis_url).await.unwrap();
    check_values(&redis_url, &log_path, keys, concurency)
        .await
        .unwrap();
}

async fn get_keys(redis_url: &str) -> RedisResult<Vec<String>> {
    println!("Start getting all keys");
    let client = redis::Client::open(redis_url)?;
    let mut con = client.get_async_connection().await?;
    let keys = con.keys("*").await?;
    Ok(keys)
}

async fn check_values(
    redis_url: &str,
    log_path: &str,
    keys: Vec<String>,
    concurrency: usize,
) -> RedisResult<()> {
    println!("Start checking all values");
    futures::stream::iter(keys)
        .map(|key| check_and_log(redis_url, log_path, key))
        .buffered(concurrency)
        .try_collect::<()>()
        .await?;
    Ok(())
}

async fn check_and_log(redis_url: &str, log_path: &str, key: String) -> RedisResult<()> {
    let client = redis::Client::open(redis_url)?;
    let mut con = client.get_async_connection().await?;
    let type_name: String = redis::cmd("TYPE").arg(&key).query_async(&mut con).await?;
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
