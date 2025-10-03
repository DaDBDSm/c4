mod encoder;

use csv::{Reader, ReaderBuilder};
use encoder::{
    ChatDataMessage, ChatsData, TradeData, TradeDataMessage, TweetData, TweetDataMessage,
};
use prost::Message;
use std::fs::{File, create_dir_all};
use std::io::Cursor;
use std::io::{BufReader, Write};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Benchmark: Proto vs C4Encoder ===\n");

    let row_counts = vec![
        50000, 75000, 100000, 250000, 500000, 750000, 1000000, 2500000, 5000000,
    ];
    let mut all_results = Vec::new();

    for &row_count in &row_counts {
        println!("\n=== Testing with {} rows ===", row_count);

        let chat_proto = load_chat_data_limited("data/chat_data.csv", row_count)?;
        let chat_c4 = load_chat_data_c4_limited("data/chat_data.csv", row_count)?;
        let trade_proto = load_trade_data_limited("data/trade_data.csv", row_count)?;
        let trade_c4 = load_trade_data_c4_limited("data/trade_data.csv", row_count)?;
        let tweet_proto = load_tweet_data_limited("data/tweets.csv", row_count)?;
        let tweet_c4 = load_tweet_data_c4_limited("data/tweets.csv", row_count)?;

        println!("Loaded {} chat messages", chat_proto.data.len());
        println!("Loaded {} trade records", trade_proto.data.len());
        println!("Loaded {} tweets", tweet_proto.data.len());

        println!("Encoding benchmark for {} rows...", row_count);

        let chat_proto_start = Instant::now();
        let chat_proto_encoded = prost::Message::encode_to_vec(&chat_proto);
        let chat_proto_time = chat_proto_start.elapsed();

        let chat_c4_start = Instant::now();
        let chat_c4_encoded = encode_chat_data_c4(&chat_c4)?;
        let chat_c4_time = chat_c4_start.elapsed();

        let trade_proto_start = Instant::now();
        let trade_proto_encoded = prost::Message::encode_to_vec(&trade_proto);
        let trade_proto_time = trade_proto_start.elapsed();

        let trade_c4_start = Instant::now();
        let trade_c4_encoded = encode_trade_data_c4(&trade_c4)?;
        let trade_c4_time = trade_c4_start.elapsed();

        let tweet_proto_start = Instant::now();
        let tweet_proto_encoded = prost::Message::encode_to_vec(&tweet_proto);
        let tweet_proto_time = tweet_proto_start.elapsed();

        let tweet_c4_start = Instant::now();
        let tweet_c4_encoded = encode_tweet_data_c4(&tweet_c4)?;
        let tweet_c4_time = tweet_c4_start.elapsed();

        println!("Decoding benchmark for {} rows...", row_count);

        let chat_proto_decode_start = Instant::now();
        let _chat_proto_decoded = ChatsData::decode(&chat_proto_encoded[..])?;
        let chat_proto_decode_time = chat_proto_decode_start.elapsed();

        let chat_c4_decode_start = Instant::now();
        let _chat_c4_decoded = decode_chat_data_c4(&chat_c4_encoded)?;
        let chat_c4_decode_time = chat_c4_decode_start.elapsed();

        let trade_proto_decode_start = Instant::now();
        let _trade_proto_decoded = TradeData::decode(&trade_proto_encoded[..])?;
        let trade_proto_decode_time = trade_proto_decode_start.elapsed();

        let trade_c4_decode_start = Instant::now();
        let _trade_c4_decoded = decode_trade_data_c4(&trade_c4_encoded)?;
        let trade_c4_decode_time = trade_c4_decode_start.elapsed();

        let tweet_proto_decode_start = Instant::now();
        let _tweet_proto_decoded = TweetData::decode(&tweet_proto_encoded[..])?;
        let tweet_proto_decode_time = tweet_proto_decode_start.elapsed();

        let tweet_c4_decode_start = Instant::now();
        let _tweet_c4_decoded = decode_tweet_data_c4(&tweet_c4_encoded)?;
        let tweet_c4_decode_time = tweet_c4_decode_start.elapsed();

        all_results.push((
            row_count,
            chat_proto_time.as_millis(),
            chat_c4_time.as_millis(),
            trade_proto_time.as_millis(),
            trade_c4_time.as_millis(),
            tweet_proto_time.as_millis(),
            tweet_c4_time.as_millis(),
            chat_proto_decode_time.as_millis(),
            chat_c4_decode_time.as_millis(),
            trade_proto_decode_time.as_millis(),
            trade_c4_decode_time.as_millis(),
            tweet_proto_decode_time.as_millis(),
            tweet_c4_decode_time.as_millis(),
        ));

        println!("Completed {} rows", row_count);
    }

    println!("\n=== SAVING RESULTS TO CSV ===");
    create_dir_all("encoders-comparison/results")?;

    let mut results_csv = File::create("encoders-comparison/results/benchmark_results.csv")?;
    writeln!(
        results_csv,
        "rows,chat_proto_encode_ms,chat_c4_encode_ms,trade_proto_encode_ms,trade_c4_encode_ms,tweet_proto_encode_ms,tweet_c4_encode_ms,chat_proto_decode_ms,chat_c4_decode_ms,trade_proto_decode_ms,trade_c4_decode_ms,tweet_proto_decode_ms,tweet_c4_decode_ms"
    )?;

    for (
        rows,
        chat_proto_encode,
        chat_c4_encode,
        trade_proto_encode,
        trade_c4_encode,
        tweet_proto_encode,
        tweet_c4_encode,
        chat_proto_decode,
        chat_c4_decode,
        trade_proto_decode,
        trade_c4_decode,
        tweet_proto_decode,
        tweet_c4_decode,
    ) in all_results
    {
        writeln!(
            results_csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            rows,
            chat_proto_encode,
            chat_c4_encode,
            trade_proto_encode,
            trade_c4_encode,
            tweet_proto_encode,
            tweet_c4_encode,
            chat_proto_decode,
            chat_c4_decode,
            trade_proto_decode,
            trade_c4_decode,
            tweet_proto_decode,
            tweet_c4_decode
        )?;
    }

    println!("\n=== BENCHMARK COMPLETED ===");

    Ok(())
}

fn load_chat_data_limited(
    path: &str,
    max_rows: usize,
) -> Result<ChatsData, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = Reader::from_reader(BufReader::new(file));
    let mut messages = Vec::new();

    for (i, result) in rdr.records().enumerate() {
        if i >= max_rows {
            break;
        }
        let record = result?;
        let message = ChatDataMessage {
            match_id: record.get(0).unwrap_or("0").parse().unwrap_or(0),
            key: record.get(1).unwrap_or("").to_string(),
            slot: record.get(2).unwrap_or("0").parse().unwrap_or(0),
            time: record.get(3).unwrap_or("0").parse().unwrap_or(0),
            unit: record.get(4).unwrap_or("").to_string(),
        };
        messages.push(message);
    }

    Ok(ChatsData { data: messages })
}

fn load_chat_data_c4_limited(
    path: &str,
    max_rows: usize,
) -> Result<Vec<ChatDataMessage>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = Reader::from_reader(BufReader::new(file));
    let mut messages = Vec::new();

    for (i, result) in rdr.records().enumerate() {
        if i >= max_rows {
            break;
        }
        let record = result?;
        let message = ChatDataMessage {
            match_id: record.get(0).unwrap_or("0").parse().unwrap_or(0),
            key: record.get(1).unwrap_or("").to_string(),
            slot: record.get(2).unwrap_or("0").parse().unwrap_or(0),
            time: record.get(3).unwrap_or("0").parse().unwrap_or(0),
            unit: record.get(4).unwrap_or("").to_string(),
        };
        messages.push(message);
    }

    Ok(messages)
}

fn load_trade_data_limited(
    path: &str,
    max_rows: usize,
) -> Result<TradeData, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = Reader::from_reader(BufReader::new(file));
    let mut records = Vec::new();

    for (i, result) in rdr.records().enumerate() {
        if i >= max_rows {
            break;
        }
        let record = result?;
        let trade = TradeDataMessage {
            timestamp: record.get(0).unwrap_or("").to_string(),
            open: record.get(1).unwrap_or("0").parse().unwrap_or(0.0),
            high: record.get(2).unwrap_or("0").parse().unwrap_or(0.0),
            low: record.get(3).unwrap_or("0").parse().unwrap_or(0.0),
            close: record.get(4).unwrap_or("0").parse().unwrap_or(0.0),
            volume: record.get(5).unwrap_or("0").parse().unwrap_or(0.0),
        };
        records.push(trade);
    }

    Ok(TradeData { data: records })
}

fn load_trade_data_c4_limited(
    path: &str,
    max_rows: usize,
) -> Result<Vec<TradeDataMessage>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = Reader::from_reader(BufReader::new(file));
    let mut records = Vec::new();

    for (i, result) in rdr.records().enumerate() {
        if i >= max_rows {
            break;
        }
        let record = result?;
        let trade = TradeDataMessage {
            timestamp: record.get(0).unwrap_or("").to_string(),
            open: record.get(1).unwrap_or("0").parse().unwrap_or(0.0),
            high: record.get(2).unwrap_or("0").parse().unwrap_or(0.0),
            low: record.get(3).unwrap_or("0").parse().unwrap_or(0.0),
            close: record.get(4).unwrap_or("0").parse().unwrap_or(0.0),
            volume: record.get(5).unwrap_or("0").parse().unwrap_or(0.0),
        };
        records.push(trade);
    }

    Ok(records)
}

fn load_tweet_data_limited(
    path: &str,
    max_rows: usize,
) -> Result<TweetData, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_reader(BufReader::new(file));
    let mut tweets = Vec::new();

    for (i, result) in rdr.records().enumerate() {
        if i >= max_rows {
            break;
        }
        match result {
            Ok(record) => {
                let tweet = TweetDataMessage {
                    id: record.get(0).unwrap_or("").to_string(),
                    user: record.get(1).unwrap_or("").to_string(),
                    fullname: record.get(2).unwrap_or("").to_string(),
                    url: record.get(3).unwrap_or("").to_string(),
                    timestamp: record.get(4).unwrap_or("").to_string(),
                    replies: record.get(5).unwrap_or("0").parse().unwrap_or(0),
                    likes: record.get(6).unwrap_or("0").parse().unwrap_or(0),
                    retweets: record.get(7).unwrap_or("0").parse().unwrap_or(0),
                    text: record.get(8).unwrap_or("").to_string(),
                };
                tweets.push(tweet);
            }
            Err(e) => {
                eprintln!(
                    "Warning: Skipping malformed record at line {}: {}",
                    i + 1,
                    e
                );
                continue;
            }
        }
    }

    Ok(TweetData { data: tweets })
}

fn load_tweet_data_c4_limited(
    path: &str,
    max_rows: usize,
) -> Result<Vec<TweetDataMessage>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_reader(BufReader::new(file));
    let mut tweets = Vec::new();

    for (i, result) in rdr.records().enumerate() {
        if i >= max_rows {
            break;
        }
        match result {
            Ok(record) => {
                let tweet = TweetDataMessage {
                    id: record.get(0).unwrap_or("").to_string(),
                    user: record.get(1).unwrap_or("").to_string(),
                    fullname: record.get(2).unwrap_or("").to_string(),
                    url: record.get(3).unwrap_or("").to_string(),
                    timestamp: record.get(4).unwrap_or("").to_string(),
                    replies: record.get(5).unwrap_or("0").parse().unwrap_or(0),
                    likes: record.get(6).unwrap_or("0").parse().unwrap_or(0),
                    retweets: record.get(7).unwrap_or("0").parse().unwrap_or(0),
                    text: record.get(8).unwrap_or("").to_string(),
                };
                tweets.push(tweet);
            }
            Err(e) => {
                eprintln!(
                    "Warning: Skipping malformed record at line {}: {}",
                    i + 1,
                    e
                );
                continue;
            }
        }
    }

    Ok(tweets)
}

fn encode_chat_data_c4(
    chat_data: &[ChatDataMessage],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut fields = Vec::new();

    for (i, message) in chat_data.iter().enumerate() {
        let message_fields = vec![
            c4encoder::Field {
                number: 1,
                value: c4encoder::Value::Int64(message.match_id),
            },
            c4encoder::Field {
                number: 2,
                value: c4encoder::Value::String(message.key.clone()),
            },
            c4encoder::Field {
                number: 3,
                value: c4encoder::Value::Int64(message.slot),
            },
            c4encoder::Field {
                number: 4,
                value: c4encoder::Value::Int64(message.time),
            },
            c4encoder::Field {
                number: 5,
                value: c4encoder::Value::String(message.unit.clone()),
            },
        ];

        fields.push(c4encoder::Field {
            number: i as u32 + 1,
            value: c4encoder::Value::Message(message_fields),
        });
    }

    let root_message = c4encoder::Value::Message(fields);
    c4encoder::encode_value(&root_message).map_err(|e| e.into())
}

fn encode_trade_data_c4(
    trade_data: &[TradeDataMessage],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut fields = Vec::new();

    for (i, record) in trade_data.iter().enumerate() {
        let record_fields = vec![
            c4encoder::Field {
                number: 1,
                value: c4encoder::Value::String(record.timestamp.clone()),
            },
            c4encoder::Field {
                number: 2,
                value: c4encoder::Value::Float64(record.open),
            },
            c4encoder::Field {
                number: 3,
                value: c4encoder::Value::Float64(record.high),
            },
            c4encoder::Field {
                number: 4,
                value: c4encoder::Value::Float64(record.low),
            },
            c4encoder::Field {
                number: 5,
                value: c4encoder::Value::Float64(record.close),
            },
            c4encoder::Field {
                number: 6,
                value: c4encoder::Value::Float64(record.volume),
            },
        ];

        fields.push(c4encoder::Field {
            number: i as u32 + 1,
            value: c4encoder::Value::Message(record_fields),
        });
    }

    let root_message = c4encoder::Value::Message(fields);
    c4encoder::encode_value(&root_message).map_err(|e| e.into())
}

fn decode_chat_data_c4(data: &[u8]) -> Result<Vec<ChatDataMessage>, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(data);
    let value = c4encoder::decode_value(&mut cursor)?;

    let mut messages = Vec::new();
    if let c4encoder::Value::Message(fields) = value {
        for field in fields {
            if let c4encoder::Value::Message(message_fields) = field.value {
                let mut match_id = 0i64;
                let mut key = String::new();
                let mut slot = 0i64;
                let mut time = 0i64;
                let mut unit = String::new();

                for msg_field in message_fields {
                    match msg_field.number {
                        1 => {
                            if let c4encoder::Value::Int64(v) = msg_field.value {
                                match_id = v;
                            }
                        }
                        2 => {
                            if let c4encoder::Value::String(v) = msg_field.value {
                                key = v;
                            }
                        }
                        3 => {
                            if let c4encoder::Value::Int64(v) = msg_field.value {
                                slot = v;
                            }
                        }
                        4 => {
                            if let c4encoder::Value::Int64(v) = msg_field.value {
                                time = v;
                            }
                        }
                        5 => {
                            if let c4encoder::Value::String(v) = msg_field.value {
                                unit = v;
                            }
                        }
                        _ => {}
                    }
                }

                messages.push(ChatDataMessage {
                    match_id,
                    key,
                    slot,
                    time,
                    unit,
                });
            }
        }
    }

    Ok(messages)
}

fn decode_trade_data_c4(data: &[u8]) -> Result<Vec<TradeDataMessage>, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(data);
    let value = c4encoder::decode_value(&mut cursor)?;

    let mut records = Vec::new();
    if let c4encoder::Value::Message(fields) = value {
        for field in fields {
            if let c4encoder::Value::Message(record_fields) = field.value {
                let mut timestamp = String::new();
                let mut open = 0.0f64;
                let mut high = 0.0f64;
                let mut low = 0.0f64;
                let mut close = 0.0f64;
                let mut volume = 0.0f64;

                for trade_field in record_fields {
                    match trade_field.number {
                        1 => {
                            if let c4encoder::Value::String(v) = trade_field.value {
                                timestamp = v;
                            }
                        }
                        2 => {
                            if let c4encoder::Value::Float64(v) = trade_field.value {
                                open = v;
                            }
                        }
                        3 => {
                            if let c4encoder::Value::Float64(v) = trade_field.value {
                                high = v;
                            }
                        }
                        4 => {
                            if let c4encoder::Value::Float64(v) = trade_field.value {
                                low = v;
                            }
                        }
                        5 => {
                            if let c4encoder::Value::Float64(v) = trade_field.value {
                                close = v;
                            }
                        }
                        6 => {
                            if let c4encoder::Value::Float64(v) = trade_field.value {
                                volume = v;
                            }
                        }
                        _ => {}
                    }
                }

                records.push(TradeDataMessage {
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                });
            }
        }
    }

    Ok(records)
}

fn encode_tweet_data_c4(
    tweet_data: &[TweetDataMessage],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut fields = Vec::new();

    for (i, tweet) in tweet_data.iter().enumerate() {
        let tweet_fields = vec![
            c4encoder::Field {
                number: 1,
                value: c4encoder::Value::String(tweet.id.clone()),
            },
            c4encoder::Field {
                number: 2,
                value: c4encoder::Value::String(tweet.user.clone()),
            },
            c4encoder::Field {
                number: 3,
                value: c4encoder::Value::String(tweet.fullname.clone()),
            },
            c4encoder::Field {
                number: 4,
                value: c4encoder::Value::String(tweet.url.clone()),
            },
            c4encoder::Field {
                number: 5,
                value: c4encoder::Value::String(tweet.timestamp.clone()),
            },
            c4encoder::Field {
                number: 6,
                value: c4encoder::Value::Int64(tweet.replies),
            },
            c4encoder::Field {
                number: 7,
                value: c4encoder::Value::Int64(tweet.likes),
            },
            c4encoder::Field {
                number: 8,
                value: c4encoder::Value::Int64(tweet.retweets),
            },
            c4encoder::Field {
                number: 9,
                value: c4encoder::Value::String(tweet.text.clone()),
            },
        ];

        fields.push(c4encoder::Field {
            number: i as u32 + 1,
            value: c4encoder::Value::Message(tweet_fields),
        });
    }

    let root_message = c4encoder::Value::Message(fields);
    c4encoder::encode_value(&root_message).map_err(|e| e.into())
}

fn decode_tweet_data_c4(data: &[u8]) -> Result<Vec<TweetDataMessage>, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(data);
    let value = c4encoder::decode_value(&mut cursor)?;

    let mut tweets = Vec::new();
    if let c4encoder::Value::Message(fields) = value {
        for field in fields {
            if let c4encoder::Value::Message(tweet_fields) = field.value {
                let mut id = String::new();
                let mut user = String::new();
                let mut fullname = String::new();
                let mut url = String::new();
                let mut timestamp = String::new();
                let mut replies = 0i64;
                let mut likes = 0i64;
                let mut retweets = 0i64;
                let mut text = String::new();

                for tweet_field in tweet_fields {
                    match tweet_field.number {
                        1 => {
                            if let c4encoder::Value::String(v) = tweet_field.value {
                                id = v;
                            }
                        }
                        2 => {
                            if let c4encoder::Value::String(v) = tweet_field.value {
                                user = v;
                            }
                        }
                        3 => {
                            if let c4encoder::Value::String(v) = tweet_field.value {
                                fullname = v;
                            }
                        }
                        4 => {
                            if let c4encoder::Value::String(v) = tweet_field.value {
                                url = v;
                            }
                        }
                        5 => {
                            if let c4encoder::Value::String(v) = tweet_field.value {
                                timestamp = v;
                            }
                        }
                        6 => {
                            if let c4encoder::Value::Int64(v) = tweet_field.value {
                                replies = v;
                            }
                        }
                        7 => {
                            if let c4encoder::Value::Int64(v) = tweet_field.value {
                                likes = v;
                            }
                        }
                        8 => {
                            if let c4encoder::Value::Int64(v) = tweet_field.value {
                                retweets = v;
                            }
                        }
                        9 => {
                            if let c4encoder::Value::String(v) = tweet_field.value {
                                text = v;
                            }
                        }
                        _ => {}
                    }
                }

                tweets.push(TweetDataMessage {
                    id,
                    user,
                    fullname,
                    url,
                    timestamp,
                    replies,
                    likes,
                    retweets,
                    text,
                });
            }
        }
    }

    Ok(tweets)
}
