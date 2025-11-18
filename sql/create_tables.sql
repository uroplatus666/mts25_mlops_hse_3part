CREATE TABLE IF NOT EXISTS default.transactions_queue
(
    transaction_time String,
    merch String,
    cat_id String,
    amount Float64,
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt64,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'transactions_topic',
         kafka_group_name = 'ch_consumer_group_v2',
         kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS default.transactions
(
    transaction_time DateTime CODEC(DoubleDelta, ZSTD(1)),
    merch String CODEC(ZSTD(1)),
    cat_id LowCardinality(String),
    amount Float64,
    name_1 String CODEC(ZSTD(1)),
    name_2 String CODEC(ZSTD(1)),
    gender LowCardinality(String),
    street String CODEC(ZSTD(1)),
    one_city LowCardinality(String),
    us_state LowCardinality(String),
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt64,
    jobs LowCardinality(String),
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8
) ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, cat_id, transaction_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.transactions_mv TO default.transactions
AS SELECT
    parseDateTimeBestEffort(transaction_time) as transaction_time,
    merch,
    cat_id,
    amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    lat,
    lon,
    population_city,
    jobs,
    merchant_lat,
    merchant_lon,
    target
FROM default.transactions_queue;