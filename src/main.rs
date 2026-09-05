use anyhow::{anyhow, Context, Result};
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::BufWriter;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

// ============================================================
// CONSTANTS
// ============================================================

const MAX_RETRY_ROUNDS: usize = 12;
const INITIAL_RETRY_DELAY_SECS: u64 = 5;
const MAX_RETRY_DELAY_SECS: u64 = 60;
const PART_SIZE: u64 = 1_000_000;
const RPC_DELAY_MS: u64 = 100;
const HTTP_TIMEOUT_SECS: u64 = 30;
const UPLOAD_RETRIES: usize = 5;
const RPC_ROTATE_BLOCKS: u64 = 20_000;
const RPC_ROTATE_WAIT_SECS: u64 = 5;

// ============================================================
// CLI CONFIG
// ============================================================

#[derive(Parser, Debug)]
#[command(author, version = "2.5.1", about = "High-performance EVM Address Extractor")]
struct Args {
    #[arg(long, env = "CHAIN", default_value = "bnb")]
    chain: String,

    #[arg(long, env = "START_BLOCK", default_value_t = 0)]
    start_block: u64,

    #[arg(long, env = "END_BLOCK", default_value_t = 0)]
    end_block: u64,

    #[arg(long, env = "BATCH_SIZE", default_value_t = 10)]
    batch_size: u64,

    #[arg(long, env = "CONCURRENCY", default_value_t = 8)]
    concurrency: usize,

    #[arg(long, env = "RELEASE_TAG")]
    release_tag: Option<String>,
}

// ============================================================
// TYPED RPC STRUCTS
// ============================================================

#[derive(Deserialize)]
struct RpcErrorDetail {
    message: String,
}

#[derive(Deserialize)]
struct TxData {
    from: Option<String>,
    to: Option<String>,
}

#[derive(Deserialize)]
struct BlockData {
    number: Option<String>,
    #[serde(default)]
    transactions: Vec<TxData>,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    id: Option<u64>,
    result: Option<T>,
    error: Option<RpcErrorDetail>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BatchPayload {
    Batch(Vec<RpcResponse<BlockData>>),
    SingleError(RpcResponse<serde_json::Value>),
}

// ============================================================
// RPC ENDPOINTS (Only Official Dataseeds & Open Nodes)
// ============================================================

fn rpc_list(chain: &str) -> Vec<String> {
    match chain.to_lowercase().as_str() {
        "bnb" | "bsc" => vec![
            "https://bsc-dataseed.binance.org/".into(),
            "https://bsc-dataseed1.binance.org/".into(),
            "https://bsc-dataseed2.binance.org/".into(),
            "https://bsc-dataseed.bnbchain.org".into(),
            "https://bsc-dataseed1.bnbchain.org".into(),
            "https://bsc-dataseed2.bnbchain.org".into(),
            "https://bsc-dataseed1.defibit.io/".into(),
            "https://bsc-dataseed2.defibit.io/".into(),
            "https://bsc-dataseed1.ninicoin.io/".into(),
            "https://bsc-dataseed2.ninicoin.io/".into(),
            "https://bsc.drpc.org".into(),
            "https://bsc.meowrpc.com".into(),
        ],
        "ethereum" | "eth" => vec![
            "https://cloudflare-eth.com".into(),
            "https://eth.drpc.org".into(),
            "https://rpc.payload.de".into(),
            "https://eth.merkle.io".into(),
        ],
        "polygon" | "matic" => vec![
            "https://polygon-rpc.com".into(),
            "https://polygon.drpc.org".into(),
            "https://polygon.meowrpc.com".into(),
        ],
        "arbitrum" | "arb" => vec![
            "https://arb1.arbitrum.io/rpc".into(),
            "https://arbitrum.drpc.org".into(),
            "https://arbitrum.meowrpc.com".into(),
        ],
        "base" => vec![
            "https://mainnet.base.org".into(),
            "https://base.drpc.org".into(),
            "https://base.meowrpc.com".into(),
        ],
        "optimism" | "op" => vec![
            "https://mainnet.optimism.io".into(),
            "https://optimism.drpc.org".into(),
            "https://optimism.meowrpc.com".into(),
        ],
        "avalanche_c" | "avalanche" | "avax" => vec![
            "https://api.avax.network/ext/bc/C/rpc".into(),
            "https://avalanche.drpc.org".into(),
        ],
        _ => vec![
            "https://bsc-dataseed.binance.org/".into(),
            "https://bsc-dataseed1.binance.org/".into(),
            "https://bsc-dataseed.bnbchain.org".into(),
            "https://bsc-dataseed1.defibit.io/".into(),
            "https://bsc-dataseed1.ninicoin.io/".into(),
        ],
    }
}

// ============================================================
// HELPERS
// ============================================================

#[inline(always)]
fn parse_hex_u64(value: &str) -> Result<u64> {
    let clean = value.trim_start_matches("0x");
    u64::from_str_radix(clean, 16).map_err(|e| anyhow!("Invalid hex '{}': {}", value, e))
}

#[inline(always)]
fn parse_address_to_bytes(addr_str: &str) -> Option<[u8; 20]> {
    let clean = addr_str.strip_prefix("0x").unwrap_or(addr_str);
    if clean.len() != 40 {
        return None;
    }
    let mut bytes = [0u8; 20];
    hex::decode_to_slice(clean, &mut bytes).ok()?;
    Some(bytes)
}

fn validate_block(block: &BlockData, expected_number: u64) -> Result<()> {
    let number_str = block
        .number
        .as_deref()
        .ok_or_else(|| anyhow!("Block {} missing number", expected_number))?;

    let actual = parse_hex_u64(number_str)?;
    if actual != expected_number {
        return Err(anyhow!(
            "Block mismatch: requested {}, received {}",
            expected_number,
            actual
        ));
    }
    Ok(())
}

fn extract_block_addresses(
    block: BlockData,
    block_number: u64,
) -> Result<(Vec<[u8; 20]>, usize)> {
    validate_block(&block, block_number)?;

    let tx_count = block.transactions.len();
    let mut addresses = Vec::with_capacity(tx_count * 2);

    for tx in block.transactions {
        if let Some(from) = tx.from.as_deref() {
            if let Some(bytes) = parse_address_to_bytes(from) {
                addresses.push(bytes);
            }
        }
        if let Some(to) = tx.to.as_deref() {
            if let Some(bytes) = parse_address_to_bytes(to) {
                addresses.push(bytes);
            }
        }
    }

    Ok((addresses, tx_count))
}

// ============================================================
// RPC REQUESTS
// ============================================================

async fn request_single_block(client: &Client, rpc: &str, block_number: u64) -> Result<BlockData> {
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [format!("0x{:x}", block_number), true],
        "id": block_number
    });

    let res = client.post(rpc).json(&payload).send().await?;
    if !res.status().is_success() {
        return Err(anyhow!("HTTP status {}", res.status()));
    }

    let parsed: RpcResponse<BlockData> = res.json().await?;
    if let Some(err) = parsed.error {
        return Err(anyhow!("RPC error: {}", err.message));
    }

    parsed
        .result
        .ok_or_else(|| anyhow!("Block {} returned null result", block_number))
}

async fn request_batch(
    client: &Client,
    rpc: &str,
    blocks: &[u64],
) -> Result<HashMap<u64, BlockData>> {
    if blocks.is_empty() {
        return Ok(HashMap::new());
    }

    let payload: Vec<_> = blocks
        .iter()
        .map(|&block| {
            json!({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [format!("0x{:x}", block), true],
                "id": block
            })
        })
        .collect();

    let res = client.post(rpc).json(&payload).send().await?;
    if !res.status().is_success() {
        return Err(anyhow!("HTTP status {}", res.status()));
    }

    let parsed_payload: BatchPayload = res.json().await?;

    let array = match parsed_payload {
        BatchPayload::Batch(arr) => arr,
        BatchPayload::SingleError(err_obj) => {
            let msg = err_obj
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "Batch error".into());
            return Err(anyhow!("RPC batch error: {}", msg));
        }
    };

    let requested: HashSet<u64> = blocks.iter().copied().collect();
    let mut results = HashMap::with_capacity(blocks.len());

    for item in array {
        if let Some(err) = item.error {
            return Err(anyhow!("RPC item error: {}", err.message));
        }

        let id = item.id.ok_or_else(|| anyhow!("Missing numeric id"))?;
        if !requested.contains(&id) {
            return Err(anyhow!("Unexpected block id {}", id));
        }

        let block = item
            .result
            .ok_or_else(|| anyhow!("Block {} returned null result", id))?;

        results.insert(id, block);
    }

    if results.len() != blocks.len() {
        return Err(anyhow!(
            "Incomplete batch: {} / {}",
            results.len(),
            blocks.len()
        ));
    }

    Ok(results)
}

// ============================================================
// BATCH FETCHING WITH RETRIES
// ============================================================

async fn fetch_batch_with_retry(
    client: &Client,
    rpcs: &[String],
    blocks: Vec<u64>,
    preferred_rpc: usize,
) -> Result<(Vec<[u8; 20]>, usize)> {
    let first = *blocks.first().unwrap_or(&0);
    let last = *blocks.last().unwrap_or(&0);

    if rpcs.is_empty() {
        return Err(anyhow!("RPC list is empty"));
    }

    let preferred = preferred_rpc % rpcs.len();

    for retry_round in 1..=MAX_RETRY_ROUNDS {
        for offset in 0..rpcs.len() {
            let rpc_index = (preferred + offset) % rpcs.len();
            let rpc = &rpcs[rpc_index];

            match request_batch(client, rpc, &blocks).await {
                Ok(mut block_map) => {
                    let mut addresses = Vec::new();
                    let mut transactions = 0usize;

                    for block_number in &blocks {
                        let block = block_map.remove(block_number).ok_or_else(|| {
                            anyhow!("Missing block {} in result map", block_number)
                        })?;

                        let (block_addresses, block_txs) =
                            extract_block_addresses(block, *block_number)?;

                        transactions += block_txs;
                        addresses.extend(block_addresses);
                    }

                    return Ok((addresses, transactions));
                }
                Err(_error) => {
                    // Failover to next RPC seamlessly
                }
            }

            sleep(Duration::from_millis(RPC_DELAY_MS)).await;
        }

        if retry_round < MAX_RETRY_ROUNDS {
            let multiplier = 2u64.saturating_pow(((retry_round - 1).min(3)) as u32);
            let delay = (INITIAL_RETRY_DELAY_SECS * multiplier).min(MAX_RETRY_DELAY_SECS);
            sleep(Duration::from_secs(delay)).await;
        }
    }

    // Fallback: Individual block requests
    let mut addresses = Vec::new();
    let mut total_transactions = 0usize;

    for block_number in &blocks {
        let mut recovered = false;

        for retry_round in 1..=MAX_RETRY_ROUNDS {
            for offset in 0..rpcs.len() {
                let rpc_index = (preferred + offset) % rpcs.len();
                let rpc = &rpcs[rpc_index];

                if let Ok(block) = request_single_block(client, rpc, *block_number).await {
                    if let Ok((block_addresses, tx_count)) =
                        extract_block_addresses(block, *block_number)
                    {
                        total_transactions += tx_count;
                        addresses.extend(block_addresses);
                        recovered = true;
                        break;
                    }
                }
                sleep(Duration::from_millis(RPC_DELAY_MS)).await;
            }

            if recovered {
                break;
            }

            if retry_round < MAX_RETRY_ROUNDS {
                let multiplier = 2u64.saturating_pow(((retry_round - 1).min(3)) as u32);
                let delay = (INITIAL_RETRY_DELAY_SECS * multiplier).min(MAX_RETRY_DELAY_SECS);
                sleep(Duration::from_secs(delay)).await;
            }
        }

        if !recovered {
            return Err(anyhow!("Block {} permanently failed on all RPCs", block_number));
        }
    }

    Ok((addresses, total_transactions))
}

// ============================================================
// FILE WRITER
// ============================================================

fn write_addresses_file(
    chain: &str,
    part_num: u32,
    start_block: u64,
    end_block: u64,
    addresses: &HashSet<[u8; 20]>,
) -> Result<String> {
    fs::create_dir_all("output")?;

    let file_name = format!(
        "output/{}_blocks_{}_to_{}_part_{:03}.csv.gz",
        chain, start_block, end_block, part_num
    );

    let file = File::create(&file_name)?;
    let buf_writer = BufWriter::with_capacity(128 * 1024, file);
    let encoder = GzEncoder::new(buf_writer, Compression::default());
    let mut writer = csv::Writer::from_writer(encoder);

    writer.write_record(["address"])?;

    let mut sorted: Vec<&[u8; 20]> = addresses.iter().collect();
    sorted.sort_unstable();

    let mut hex_buf = String::with_capacity(42);
    for addr in sorted {
        hex_buf.clear();
        hex_buf.push_str("0x");
        hex_buf.push_str(&hex::encode(addr));
        writer.write_record([&hex_buf])?;
    }

    writer.flush()?;

    println!("\n==============================================");
    println!("PART COMPLETED: {}", file_name);
    println!("Unique Addresses: {}", addresses.len());
    println!("==============================================\n");

    Ok(file_name)
}

// ============================================================
// UPLOADER
// ============================================================

async fn upload_to_release(tag: &str, file_name: &str) -> bool {
    println!("\nUploading {} to release {}...", file_name, tag);

    for attempt in 1..=UPLOAD_RETRIES {
        let status = tokio::process::Command::new("gh")
            .args(["release", "upload", tag, file_name, "--clobber"])
            .status()
            .await;

        match status {
            Ok(s) if s.success() => {
                println!("Successfully uploaded: {}", file_name);
                let _ = fs::remove_file(file_name);
                return true;
            }
            _ => {
                eprintln!("Upload attempt {}/{} failed.", attempt, UPLOAD_RETRIES);
                if attempt < UPLOAD_RETRIES {
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    eprintln!("FAILED to upload {} after {} attempts.", file_name, UPLOAD_RETRIES);
    false
}

async fn get_latest_block(client: &Client, rpcs: &[String]) -> Result<u64> {
    loop {
        for (index, rpc) in rpcs.iter().enumerate() {
            let res = client
                .post(rpc)
                .json(&json!({
                    "jsonrpc": "2.0",
                    "method": "eth_blockNumber",
                    "params": [],
                    "id": 1
                }))
                .send()
                .await;

            if let Ok(response) = res {
                if response.status().is_success() {
                    if let Ok(parsed) = response.json::<RpcResponse<String>>().await {
                        if let Some(result) = parsed.result {
                            if let Ok(block) = parse_hex_u64(&result) {
                                println!("Connected to RPC #{} ({}) | Latest block: {}", index + 1, rpc, block);
                                return Ok(block);
                            }
                        }
                    }
                }
            }
        }

        println!("All RPCs failed for block height check. Retrying in 15s...");
        sleep(Duration::from_secs(15)).await;
    }
}

// ============================================================
// MAIN PIPELINE
// ============================================================

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.start_block > args.end_block {
        return Err(anyhow!("start-block cannot be greater than end-block"));
    }
    if args.batch_size == 0 || args.concurrency == 0 {
        return Err(anyhow!("batch-size and concurrency must be > 0"));
    }

    let rpcs = rpc_list(&args.chain);

    // Standard Browser User-Agent avoids Cloudflare 403 blocks
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(args.concurrency * 2)
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        .build()?;

    let client = Arc::new(client);
    let rpcs = Arc::new(rpcs);

    let latest_block = get_latest_block(&client, &rpcs).await?;
    if args.end_block > latest_block {
        return Err(anyhow!(
            "Requested end block {} > chain latest block {}",
            args.end_block,
            latest_block
        ));
    }

    let mut part_start = args.start_block;
    let mut part_num: u32 = 1;

    while part_start <= args.end_block {
        let part_end = part_start
            .saturating_add(PART_SIZE - 1)
            .min(args.end_block);

        println!("\n>>> STARTING PART {:03}: {} -> {}", part_num, part_start, part_end);

        let mut unique_addresses: HashSet<[u8; 20]> = HashSet::new();
        let mut part_total_blocks = 0u64;
        let mut part_total_transactions = 0u64;
        let mut part_total_addresses = 0u64;

        let mut segment_start = part_start;
        let mut segment_number: u64 = 0;

        while segment_start <= part_end {
            let segment_end = segment_start
                .saturating_add(RPC_ROTATE_BLOCKS - 1)
                .min(part_end);

            let preferred_rpc = (segment_number as usize) % rpcs.len();

            println!(
                "\nSegment #{} | Blocks: {} -> {} | Active Base RPC: {}",
                segment_number + 1,
                segment_start,
                segment_end,
                rpcs[preferred_rpc]
            );

            let mut batches = Vec::new();
            let mut current = segment_start;

            while current <= segment_end {
                let batch_end = current
                    .saturating_add(args.batch_size - 1)
                    .min(segment_end);
                batches.push((current..=batch_end).collect::<Vec<_>>());
                if batch_end == u64::MAX {
                    break;
                }
                current = batch_end + 1;
            }

            let total_batches = batches.len();
            let mut batch_stream = stream::iter(batches)
                .map(|batch| {
                    let client = Arc::clone(&client);
                    let rpcs = Arc::clone(&rpcs);
                    async move {
                        let first = *batch.first().unwrap();
                        let last = *batch.last().unwrap();
                        let result =
                            fetch_batch_with_retry(&client, &rpcs, batch, preferred_rpc).await;
                        (first, last, result)
                    }
                })
                .buffer_unordered(args.concurrency);

            let mut processed_batches = 0usize;
            let mut segment_blocks = 0u64;

            while let Some((first, last, result)) = batch_stream.next().await {
                let (addresses, tx_count) = result.with_context(|| {
                    format!("Permanent error on batch {}-{}", first, last)
                })?;

                part_total_transactions += tx_count as u64;
                part_total_addresses += addresses.len() as u64;
                unique_addresses.extend(addresses);

                processed_batches += 1;
                let current_blocks = last.saturating_sub(first) + 1;
                segment_blocks += current_blocks;
                part_total_blocks += current_blocks;

                if processed_batches % 100 == 0 || processed_batches == total_batches {
                    let pct = (processed_batches as f64 / total_batches as f64) * 100.0;
                    println!(
                        "Progress: {}/{} ({:.2}%) | Blocks: {} | Txs: {} | Unique Addrs: {}",
                        processed_batches,
                        total_batches,
                        pct,
                        segment_blocks,
                        part_total_transactions,
                        unique_addresses.len()
                    );
                }
            }

            if segment_end < part_end {
                println!("\nSegment done. Cooling down for {}s...", RPC_ROTATE_WAIT_SECS);
                sleep(Duration::from_secs(RPC_ROTATE_WAIT_SECS)).await;
                segment_number = segment_number.saturating_add(1);
            }

            if segment_end == u64::MAX {
                break;
            }
            segment_start = segment_end + 1;
        }

        let file_name = write_addresses_file(
            &args.chain,
            part_num,
            part_start,
            part_end,
            &unique_addresses,
        )?;

        if let Some(ref tag) = args.release_tag {
            if !upload_to_release(tag, &file_name).await {
                return Err(anyhow!("Failed uploading {} to release", file_name));
            }
        }

        if part_end == u64::MAX {
            break;
        }
        part_start = part_end + 1;
        part_num += 1;
    }

    println!("\nDone. All blocks extracted successfully.");
    Ok(())
}
