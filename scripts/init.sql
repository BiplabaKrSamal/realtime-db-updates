CREATE TABLE IF NOT EXISTS orders (
    id            SERIAL PRIMARY KEY,
    customer_name VARCHAR(255)  NOT NULL,
    product_name  VARCHAR(255)  NOT NULL,
    status        VARCHAR(20)   NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'shipped', 'delivered')),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_customer   ON orders (customer_name);

CREATE PUBLICATION IF NOT EXISTS orders_pub FOR TABLE orders;

INSERT INTO orders (customer_name, product_name, status) VALUES
    ('Alice Johnson',  'Wireless Headphones', 'pending'),
    ('Bob Smith',      'Mechanical Keyboard',  'shipped'),
    ('Carol Williams', 'USB-C Hub',            'delivered'),
    ('David Brown',    'Standing Desk Mat',    'pending'),
    ('Eva Martinez',   '4K Webcam',            'shipped')
ON CONFLICT DO NOTHING;
