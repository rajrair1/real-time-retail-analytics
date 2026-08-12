CREATE TABLE IF NOT EXISTS dim_product (
  product_id text PRIMARY KEY, product_name text, category text
);
CREATE TABLE IF NOT EXISTS dim_store (
  store_id text PRIMARY KEY, store_name text, region text
);
CREATE TABLE IF NOT EXISTS fact_sales (
  event_id uuid PRIMARY KEY,
  event_time timestamptz NOT NULL,
  date_key integer NOT NULL,
  customer_id text,
  product_id text NOT NULL,
  store_id text NOT NULL,
  quantity integer NOT NULL CHECK (quantity > 0),
  unit_price numeric(12,2) NOT NULL CHECK (unit_price >= 0),
  discount_rate numeric(5,4) NOT NULL CHECK (discount_rate BETWEEN 0 AND 1),
  payment_method text,
  net_amount numeric(14,2) NOT NULL,
  loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_sales_event_time ON fact_sales(event_time);
CREATE INDEX IF NOT EXISTS ix_fact_sales_store_date ON fact_sales(store_id, date_key);
