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
--  postgres=# \i [path to file]/stored_procs.sql
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- Function: f_tr_exchange()
-- DROP FUNCTION f_tr_exchange();
CREATE OR REPLACE FUNCTION f_tr_exchange() RETURNS trigger AS $f_tr_exchange$
    BEGIN
      IF NEW.abbrev IS NULL THEN
         RAISE EXCEPTION 't_exchange cannot have null abbrev';
      END IF;
      IF NEW.exchange_name IS NULL THEN
         RAISE EXCEPTION 't_exchange cannot have null exchange_name';
      END IF;

      -- Set create timestamp
      IF TG_OP = 'INSERT'
      THEN
         NEW.create_date := current_timestamp;
      END IF;
      
      IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE'
      THEN
         NEW.update_date  := current_timestamp;
      END IF;
         
      RETURN NEW;
    END;
$f_tr_exchange$ LANGUAGE plpgsql;

CREATE TRIGGER tr_exchange BEFORE INSERT OR UPDATE ON t_exchange
    FOR EACH ROW EXECUTE PROCEDURE f_tr_exchange();
-- -------------------------------------------------------------------------------------------------
-- Function: f_exchange_ins_upd(text, text, text, text, text, text, text, integer, text, text, integer, timestamp without time zone)
-- DROP FUNCTION f_exchange_ins_upd(text, text, text, text, text, text, text, integer, text, text, integer, timestamp without time zone);
CREATE OR REPLACE FUNCTION f_exchange_ins_upd(
    abbrev_i              TEXT,
    exchange_name_i       TEXT)
  RETURNS bigint AS
$BODY$
DECLARE
  var_id            BIGINT      := 0;
  var_exchange_row  t_exchange%ROWTYPE;
BEGIN
  SELECT id
    INTO var_exchange_row
    FROM t_exchange
   WHERE abbrev         = abbrev_i
     AND exchange_name  = exchange_name_i;

  var_id := var_exchange_row.id;
  
  IF var_id IS NULL OR var_id = 0
  THEN
    var_id   := NEXTVAL('seq_exchange');
    INSERT INTO t_exchange (
        id,
        abbrev,
        exchange_name
    )
    VALUES (
        var_id,
        abbrev_i,
        exchange_name_i
    );
  ELSE
    UPDATE t_exchange SET
        abbrev          = abbrev_i,
        exchange_name   = exchange_name_i
    WHERE
        id = var_id;
  END IF;

  RETURN var_id;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION f_exchange_ins_upd(
    abbrev_i        TEXT,
    exchange_name_i TEXT) OWNER TO trade_forecast;
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- Function: f_tr_symbol()
-- DROP FUNCTION f_tr_symbol();
CREATE OR REPLACE FUNCTION f_tr_symbol() RETURNS trigger AS $f_tr_symbol$
    BEGIN
      IF NEW.ticker IS NULL THEN
         RAISE EXCEPTION 't_symbol cannot have null ticker';
      END IF;
      IF NEW.company_name IS NULL THEN
         RAISE EXCEPTION 't_symbol cannot have null company_name';
      END IF;
      IF NEW.sector IS NULL THEN
         RAISE EXCEPTION 't_symbol cannot have null sector';
      END IF;
      IF NEW.industry IS NULL THEN
         RAISE EXCEPTION 't_symbol cannot have null industry';
      END IF;
      IF NEW.exchange_id IS NULL THEN
         RAISE EXCEPTION 't_symbol cannot have null exchange_id';
      END IF;

      -- Set create timestamp
      IF TG_OP = 'INSERT'
      THEN
         NEW.create_date := current_timestamp;
      END IF;
      
      IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE'
      THEN
         NEW.update_date  := current_timestamp;
      END IF;
         
      RETURN NEW;
    END;
$f_tr_symbol$ LANGUAGE plpgsql;
-- -------------------------------------------------------------------------------------------------
CREATE TRIGGER tr_symbol BEFORE INSERT OR UPDATE ON t_symbol
    FOR EACH ROW EXECUTE PROCEDURE f_tr_symbol();
-- -------------------------------------------------------------------------------------------------
-- Function: f_symbol_ins_upd(text, text, text, text, text, text, text, integer, text, text, integer, timestamp without time zone)
-- DROP FUNCTION f_symbol_ins_upd(text, text, text, text, text, text, text, integer, text, text, integer, timestamp without time zone);
CREATE OR REPLACE FUNCTION f_symbol_ins_upd(
    ticker_i        TEXT,
    company_name_i  TEXT,
    sector_i        TEXT,
    industry_i      TEXT,
    exchange_i      TEXT)
  RETURNS bigint AS
$BODY$
DECLARE
  var_id            BIGINT      := 0;
  var_exchange_id   BIGINT      := 0;
  var_exchange_row  t_exchange%ROWTYPE;
  var_symbol_row    t_symbol%ROWTYPE;
BEGIN
    SELECT id
    INTO var_exchange_row
    FROM t_exchange
    WHERE abbrev = exchange_i;

    var_exchange_id = var_exchange_row.id;

  SELECT id
    INTO var_symbol_row
    FROM t_symbol
   WHERE ticker         = ticker_i
     AND company_name   = company_name_i
     AND exchange_id    = var_exchange_id;

  var_id := var_symbol_row.id;
  
  IF var_id IS NULL OR var_id = 0
  THEN
    var_id   := NEXTVAL('seq_symbol');
    INSERT INTO t_symbol (
        id,
        ticker,
        company_name,
        sector,
        industry,
        exchange_id
    )
    VALUES (
        var_id,
        ticker_i,
        company_name_i,
        sector_i,
        industry_i,
        var_exchange_id
    );
  ELSE
    UPDATE t_symbol SET
        ticker          = ticker_i,
        company_name    = company_name_i,
        sector          = sector_i,
        industry        = industry_i,
        exchange_id     = var_exchange_id
    WHERE
        id = var_id;
  END IF;

  RETURN var_id;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION f_symbol_ins_upd(
    ticker_i        TEXT,
    company_name_i  TEXT,
    sector_i        TEXT,
    industry_i      TEXT,
    exchange_id_i   TEXT) OWNER TO trade_forecast;
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- Function: f_tr_daily_price()
-- DROP FUNCTION f_tr_daily_price();
CREATE OR REPLACE FUNCTION f_tr_daily_price() RETURNS trigger AS $f_tr_daily_price$
    BEGIN
      IF NEW.price_date IS NULL THEN
         RAISE EXCEPTION 't_daily_price cannot have null price_date';
      END IF;
      IF NEW.open_price IS NULL THEN
         RAISE EXCEPTION 't_daily_price cannot have null open_price';
      END IF;
      IF NEW.high_price IS NULL THEN
         RAISE EXCEPTION 't_daily_price cannot have null high_price';
      END IF;
      IF NEW.low_price IS NULL THEN
         RAISE EXCEPTION 't_daily_price cannot have null low_price';
      END IF;
      IF NEW.close_price IS NULL THEN
         RAISE EXCEPTION 't_daily_price cannot have null close_price';
      END IF;
      IF NEW.adj_close_price IS NULL THEN
         RAISE EXCEPTION 't_daily_price cannot have null adj_close_price';
      END IF;
      IF NEW.percent_change IS NULL THEN
         RAISE EXCEPTION 't_daily_price cannot have null percent_change';
      END IF;
      IF NEW.volume IS NULL THEN
         RAISE EXCEPTION 't_daily_price cannot have null volume';
      END IF;
      IF NEW.symbol_id IS NULL THEN
         RAISE EXCEPTION 't_daily_price cannot have null symbol_id';
      END IF;

      -- Set create timestamp
      IF TG_OP = 'INSERT'
      THEN
         NEW.create_date := current_timestamp;
      END IF;
      
      IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE'
      THEN
         NEW.update_date  := current_timestamp;
      END IF;
         
      RETURN NEW;
    END;
$f_tr_daily_price$ LANGUAGE plpgsql;

CREATE TRIGGER tr_daily_price BEFORE INSERT OR UPDATE ON t_daily_price
    FOR EACH ROW EXECUTE PROCEDURE f_tr_daily_price();
-- -------------------------------------------------------------------------------------------------
-- Function: f_daily_price_ins_upd(text, text, text, text, text, text, text, integer, text, text, integer, timestamp without time zone)
-- DROP FUNCTION f_daily_price_ins_upd(text, text, text, text, text, text, text, integer, text, text, integer, timestamp without time zone);
CREATE OR REPLACE FUNCTION f_daily_price_ins_upd(
    symbol_id_i         BIGINT,
    price_date_i        TIMESTAMP,
    open_price_i        numeric,
    high_price_i        numeric,
    low_price_i         numeric,
    close_price_i       numeric,
    adj_close_price_i   numeric,
    percent_change_i    numeric,
    volume_i            BIGINT)
  RETURNS bigint AS
$BODY$
DECLARE
  var_id      BIGINT      := 0;
  var_daily_price_row    t_daily_price%ROWTYPE;
BEGIN
  SELECT id
    INTO var_daily_price_row
    FROM t_daily_price
   WHERE price_date = price_date_i
     AND symbol_id  = symbol_id_i;

  var_id := var_daily_price_row.id;
  
  IF var_id IS NULL OR var_id = 0
  THEN
    var_id   := NEXTVAL('seq_daily_price');
    INSERT INTO t_daily_price (
        id,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        adj_close_price,
        percent_change,
        volume,
        symbol_id
    )
    VALUES (
        var_id,
        price_date_i,
        open_price_i,
        high_price_i,
        low_price_i,
        close_price_i,
        adj_close_price_i,
        percent_change_i,
        volume_i,
        symbol_id_i
    );
  ELSE
    UPDATE t_daily_price SET
        price_date      = price_date_i,
        open_price      = open_price_i,
        high_price      = high_price_i,
        low_price       = low_price_i,
        close_price     = close_price_i,
        adj_close_price = adj_close_price_i,
        percent_change  = percent_change_i,
        volume          = volume_i,
        symbol_id       = symbol_id_i
    WHERE
        id = var_id;
  END IF;

  RETURN var_id;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION f_daily_price_ins_upd(
    symbol_id_i         BIGINT,
    price_date_i        TIMESTAMP,
    open_price_i        numeric,
    high_price_i        numeric,
    low_price_i         numeric,
    close_price_i       numeric,
    adj_close_price_i   numeric,
    percent_change_i    numeric,
    volume_i            BIGINT) OWNER TO trade_forecast;
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- add some initial data
SELECT * FROM f_exchange_ins_upd('NYSE', 'New York Stock Exchange');
SELECT * FROM f_exchange_ins_upd('NASDAQ', 'National Association of Securities Dealers Automated Quotations');
SELECT * FROM f_exchange_ins_upd('AMEX', 'American Stock Exchange');
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------

