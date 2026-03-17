CREATE MATERIALIZED VIEW default.mv_user_item_matrix TO default.user_item_matrix AS
SELECT user_id,
    item_id,
    sum(
        CASE
            event_type
            WHEN 'view' THEN 1.0
            WHEN 'click' THEN 2.0
            WHEN 'add_to_cart' THEN 5.0
            WHEN 'purchase' THEN 10.0
            ELSE 0.0
        END
    ) AS interaction_score,
    max(event_time) AS last_interaction
FROM default.enriched_events
GROUP BY user_id,
    item_id;
CREATE TABLE default.enriched_events (
    `user_id` UInt64,
    -- Chuyển sang số nếu ID là số
    `session_id` String,
    `event_type` LowCardinality(String),
    -- Tối ưu nén cho loại event
    `item_id` UInt64,
    -- Chuyển sang số
    `category` LowCardinality(String),
    -- Tối ưu nén cho category
    `price` Float32,
    `event_time` DateTime64(3)
) ENGINE = ReplacingMergeTree(event_time) -- Hỗ trợ xóa trùng tự động
PARTITION BY toYYYYMM(event_time) -- Chia dữ liệu theo tháng
ORDER BY (event_time, user_id) -- Tối ưu cho query theo thời gian
    SETTINGS index_granularity = 8192;