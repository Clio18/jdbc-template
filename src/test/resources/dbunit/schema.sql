CREATE TABLE IF NOT EXISTS PRODUCTS
(
    `id`       SERIAL PRIMARY KEY,
    `name`    VARCHAR(100),
    `price`    NUMERIC,
    `creation_date` TIMESTAMP,
    `description`    VARCHAR(500)
);