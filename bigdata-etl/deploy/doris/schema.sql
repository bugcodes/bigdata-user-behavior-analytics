create database if not exists bigdata_analytics;
use bigdata_analytics;

create table if not exists dwd_event_detail (
    event_date date not null,
    event_id varchar(128) not null,
    app_id varchar(64) not null,
    user_id varchar(128),
    device_id varchar(128),
    session_id varchar(128),
    event_name varchar(128),
    event_time bigint,
    receive_time bigint,
    process_time bigint,
    process_latency_ms bigint,
    valid boolean,
    ip varchar(64),
    user_agent string,
    properties json
)
unique key(event_date, event_id, app_id)
partition by range(event_date) ()
distributed by hash(event_id) buckets 32
properties (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "enable_unique_key_merge_on_write" = "true"
);

create table if not exists dws_event_1m (
    app_id varchar(64) not null,
    event_name varchar(128) not null,
    window_start datetime not null,
    window_end datetime,
    pv bigint,
    uv bigint,
    users bigint,
    avg_latency_ms double
)
unique key(app_id, event_name, window_start)
partition by range(window_start) ()
distributed by hash(app_id, event_name) buckets 16
properties (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "enable_unique_key_merge_on_write" = "true"
);

create table if not exists ads_checkout_funnel (
    app_id varchar(64) not null,
    stat_date date not null,
    step_order int not null,
    step_name varchar(64),
    users bigint,
    conversion_rate double
)
unique key(app_id, stat_date, step_order)
distributed by hash(app_id) buckets 8
properties (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);
