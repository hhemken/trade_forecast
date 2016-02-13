-- -------------------------------------------------------------------------------------------------
-- :mode=pl-sql:    (jedit)
-- -------------------------------------------------------------------------------------------------
-- Modified from:
-- https://www.quantstart.com/articles/Securities-Master-Database-with-MySQL-and-Python
-- Using postgresql instead of mysql
-- -------------------------------------------------------------------------------------------------
-- initial commands to create user and database; this all needs to run in psql as the postgres user,
-- e.g.
--  sudo su - postgres
--  psql
--  postgres=# \i [path to file]/schema.sql
-- WARNING: This will of course erase everything that might already be there.
CREATE USER trade_forecast WITH PASSWORD '867-5309';
CREATE DATABASE trade_forecast OWNER trade_forecast;
\c trade_forecast;
CREATE LANGUAGE plpgsql;
-- -------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS t_exchange;
CREATE TABLE t_exchange (
    id                  BIGINT PRIMARY KEY,
    abbrev              TEXT NOT NULL,
    exchange_name       TEXT NOT NULL,
    create_date         TIMESTAMP,
    update_date         TIMESTAMP
);
CREATE INDEX ix_exchange_create_date      ON t_exchange(create_date);
CREATE INDEX ix_exchange_update_date      ON t_exchange(update_date);
DROP SEQUENCE IF EXISTS seq_exchange;
CREATE SEQUENCE seq_exchange MINVALUE 1;
GRANT ALL ON TABLE t_exchange TO trade_forecast;
GRANT ALL ON SEQUENCE seq_exchange TO trade_forecast;
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- "Symbol","Name","LastSale","MarketCap","IPOyear","Sector","industry","Summary Quote",
DROP TABLE IF EXISTS t_symbol;
CREATE TABLE t_symbol (
    id              BIGINT PRIMARY KEY,
    ticker          TEXT NOT NULL,
    company_name    TEXT NOT NULL,
    sector          TEXT NOT NULL,
    industry        TEXT NOT NULL,
    create_date     TIMESTAMP,
    update_date     TIMESTAMP,
    exchange_id     BIGINT REFERENCES t_exchange
);
CREATE INDEX ix_symbol_create_date      ON t_symbol(create_date);
CREATE INDEX ix_symbol_update_date      ON t_symbol(update_date);
DROP SEQUENCE IF EXISTS seq_symbol;
CREATE SEQUENCE seq_symbol MINVALUE 1;
GRANT ALL ON TABLE t_symbol TO trade_forecast;
GRANT ALL ON SEQUENCE seq_symbol TO trade_forecast;
-- -------------------------------------------------------------------------------------------------
-- {'2013-01-03': {'Adj Close': '723.67',
--                 'Close': '723.67',
--                 'High': '731.93',
--                 'Low': '720.72',
--                 'Open': '724.93',
--                 'Volume': '2318200'},
DROP TABLE IF EXISTS t_daily_price;
CREATE TABLE t_daily_price (
    id                  BIGINT PRIMARY KEY,
    price_date          TIMESTAMP NOT NULL,
    open_price          numeric NULL,
    high_price          numeric NULL,
    low_price           numeric NULL,
    close_price         numeric NULL,
    adj_close_price     numeric NULL,
    percent_change      numeric NULL,
    volume              BIGINT NULL,
    create_date         TIMESTAMP,
    update_date         TIMESTAMP,
    symbol_id           BIGINT REFERENCES t_symbol
);
CREATE INDEX ix_daily_price_create_date      ON t_daily_price(create_date);
CREATE INDEX ix_daily_price_update_date      ON t_daily_price(update_date);
DROP SEQUENCE IF EXISTS seq_daily_price;
CREATE SEQUENCE seq_daily_price MINVALUE 1;
GRANT ALL ON TABLE t_daily_price TO trade_forecast;
GRANT ALL ON SEQUENCE seq_daily_price TO trade_forecast;
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS t_download_results;
CREATE TABLE t_download_results (
    id              BIGINT PRIMARY KEY,
    successes       numeric NOT NULL,
    failures        numeric NOT NULL,
    create_date     TIMESTAMP,
    update_date     TIMESTAMP,
    symbol_id       BIGINT REFERENCES t_download_results
);
CREATE INDEX ix_download_results_create_date      ON t_download_results(create_date);
CREATE INDEX ix_download_results_update_date      ON t_download_results(update_date);
DROP SEQUENCE IF EXISTS seq_download_results;
CREATE SEQUENCE seq_download_results MINVALUE 1;
GRANT ALL ON TABLE t_download_results TO trade_forecast;
GRANT ALL ON SEQUENCE seq_download_results TO trade_forecast;
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------



