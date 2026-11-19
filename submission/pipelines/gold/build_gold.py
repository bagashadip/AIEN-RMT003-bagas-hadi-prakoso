"""Layer Gold: bangun 5 tabel kontrak (gold.sql) dari Silver, lalu rekonsiliasi.

Gold hanya membaca Silver. Setiap sumber dijumlahkan dulu sesuai grain-nya sendiri
(pre-aggregation), baru di-join -> tidak ada angka yang terhitung dobel (fan-out).
Setiap run, Gold dibangun ulang dari nol (TRUNCATE + INSERT dalam 1 transaksi).
Nomor "aturan" di komentar merujuk ke daftar aturan di GUIDE_milestone1.md.

Keputusan soal uang:
- discount_amount = diskon item + diskon promo (aturan 16). Sudah dibuktikan di data:
  uang yang di-capture = GMV - kedua diskon + ongkir, untuk semua order yang dibayar.
- refunded_amount = refund COMPLETED, maksimal sebesar uang yang di-capture (aturan 27).
- Order CANCELLED tidak pernah dibayar (aturan 23): tetap ada di order_360 dan dihitung
  sebagai order yang dibuat, tapi net_revenue = 0 dan tidak ikut gross revenue,
  maupun unit terjual (aturan 37).
- return_rate = order yang punya return event / semua resolved order; AOV = net_revenue /
  semua resolved order. "Resolved order" = 1 order hasil dedup (grain order_360 di katalog
  engine), sesuai definisi README bagian 4 (aturan 29).

Isi file:
- build_gold     -> isi 5 tabel Gold.
- validate_gold  -> rekonsiliasi angka Gold vs Silver; lolos -> run ditandai SUCCESS.
"""

from shared.db import connect

GOLD_TABLES = [
    "gold.order_360",
    "gold.product_daily",
    "gold.customer_daily",
    "gold.channel_campaign_daily",
    "gold.executive_kpis_daily",
]

# ---------- 1. gold.order_360: 1 baris per order ----------
# CTE items/promos/payments/refunds/last_return masing-masing dijumlah PER ORDER dulu,
# baru di-LEFT JOIN ke silver.orders. Order tanpa data di suatu sumber -> COALESCE jadi 0.
ORDER_360 = """
INSERT INTO gold.order_360 (
    order_id, customer_id, order_date, sales_channel, store_id,
    gross_merchandise_value, discount_amount, shipping_revenue,
    captured_payment_amount, refunded_amount, net_revenue,
    item_count, unit_quantity, order_status, payment_status, return_status,
    promotion_count, first_order_flag, pipeline_run_id
)
WITH items AS (
    SELECT order_id,
           SUM(quantity * unit_price) AS gmv,
           SUM(item_discount_amount)  AS item_discount,
           COUNT(*)                   AS item_count,
           SUM(quantity)              AS unit_quantity
    FROM silver.order_items
    GROUP BY order_id
),
promos AS (
    SELECT order_id, SUM(discount_amount) AS promo_discount, COUNT(*) AS promotion_count
    FROM silver.order_promotions
    GROUP BY order_id
),
payments AS (
    SELECT order_id, SUM(amount) AS captured           -- split payment dijumlah (aturan 21)
    FROM silver.payment_events
    WHERE event_type = 'PAYMENT_CAPTURED'
    GROUP BY order_id
),
refunds AS (
    SELECT order_id, SUM(amount) AS refunded            -- hanya COMPLETED (aturan 25)
    FROM silver.refund_events
    WHERE event_type = 'REFUND_COMPLETED'
    GROUP BY order_id
),
last_return AS (                                         -- tahap return terakhir (aturan 28)
    SELECT DISTINCT ON (order_id) order_id, event_type
    FROM silver.return_events
    ORDER BY order_id, occurred_at_utc DESC, event_id DESC
),
base AS (
    SELECT o.*,
           COALESCE(i.gmv, 0) AS gmv,
           COALESCE(i.item_discount, 0) + COALESCE(pr.promo_discount, 0) AS discount,
           COALESCE(p.captured, 0) AS captured,
           LEAST(COALESCE(r.refunded, 0), COALESCE(p.captured, 0)) AS refunded,
           COALESCE(i.item_count, 0) AS item_count,
           COALESCE(i.unit_quantity, 0) AS unit_quantity,
           COALESCE(pr.promotion_count, 0) AS promotion_count,
           lr.event_type AS last_return_event
    FROM silver.orders o
    LEFT JOIN items i     ON i.order_id = o.order_id
    LEFT JOIN promos pr   ON pr.order_id = o.order_id
    LEFT JOIN payments p  ON p.order_id = o.order_id
    LEFT JOIN refunds r   ON r.order_id = o.order_id
    LEFT JOIN last_return lr ON lr.order_id = o.order_id
)
SELECT
    order_id,
    customer_id,
    (ordered_at_utc AT TIME ZONE 'UTC')::date,
    sales_channel,
    store_id,
    gmv,
    discount,
    shipping_revenue,
    captured,
    refunded,
    CASE WHEN status = 'CANCELLED' THEN 0
         ELSE gmv - discount + shipping_revenue - refunded END,
    item_count,
    unit_quantity,
    status,
    CASE WHEN captured = 0 THEN 'UNPAID'
         WHEN refunded >= captured THEN 'REFUNDED'
         WHEN refunded > 0 THEN 'PARTIALLY_REFUNDED'
         ELSE 'CAPTURED' END,
    COALESCE(REPLACE(last_return_event, 'RETURN_', ''), 'NONE'),
    promotion_count,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ordered_at_utc, order_id) = 1,
    %s
FROM base
"""

# ---------- 2. gold.product_daily: 1 baris per produk per hari ----------
# Kunci (keys) = gabungan hari yang ada penjualan + hari yang ada snapshot inventory,
# supaya hari tanpa penjualan tetap punya baris (penting untuk stockout).
PRODUCT_DAILY = """
INSERT INTO gold.product_daily (
    product_id, metric_date, active_category, units_sold, order_count,
    gross_revenue, net_revenue, refunded_units, available_inventory,
    reserved_inventory, stockout_flag, promotion_count, pipeline_run_id
)
WITH sold AS (                                -- produk dikenal, order non-CANCELLED (aturan 7, 37)
    SELECT i.product_id, o.order_date AS metric_date, i.order_id, i.quantity,
           i.quantity * i.unit_price AS gross, i.item_discount_amount AS item_discount,
           (o.payment_status = 'REFUNDED') AS fully_refunded
    FROM silver.order_items i
    JOIN gold.order_360 o ON o.order_id = i.order_id
    WHERE i.is_known_product AND o.order_status <> 'CANCELLED'
),
sales AS (
    SELECT product_id, metric_date,
           SUM(quantity) AS units_sold,
           COUNT(DISTINCT order_id) AS order_count,
           SUM(gross) AS gross_revenue,
           SUM(gross - item_discount) AS net_revenue,
           SUM(quantity) FILTER (WHERE fully_refunded) AS refunded_units
    FROM sold
    GROUP BY product_id, metric_date
),
promos AS (
    SELECT s.product_id, s.metric_date, COUNT(DISTINCT op.promotion_id) AS promotion_count
    FROM (SELECT DISTINCT product_id, metric_date, order_id FROM sold) s
    JOIN silver.order_promotions op ON op.order_id = s.order_id
    GROUP BY s.product_id, s.metric_date
),
inventory AS (                                -- hanya lokasi yang melapor (aturan 34, 35)
    SELECT product_id, snapshot_date AS metric_date,
           SUM(available_quantity) AS available,
           SUM(reserved_quantity) AS reserved,
           BOOL_OR(available_quantity = 0) AS stockout
    FROM silver.inventory_snapshots
    GROUP BY product_id, snapshot_date
),
keys AS (
    SELECT product_id, metric_date FROM sales
    UNION
    SELECT product_id, metric_date FROM inventory
)
SELECT
    k.product_id,
    k.metric_date,
    (SELECT c.category_name FROM silver.product_categories c          -- versi kategori yang berlaku hari itu (aturan 10)
     WHERE c.product_id = k.product_id
       AND c.valid_from_utc < (k.metric_date + 1)::timestamp AT TIME ZONE 'UTC'
       AND c.valid_to_utc >= k.metric_date::timestamp AT TIME ZONE 'UTC'
     ORDER BY c.valid_from_utc DESC
     LIMIT 1),
    COALESCE(s.units_sold, 0),
    COALESCE(s.order_count, 0),
    COALESCE(s.gross_revenue, 0),
    COALESCE(s.net_revenue, 0),
    COALESCE(s.refunded_units, 0),
    inv.available,
    inv.reserved,
    inv.stockout,
    COALESCE(pm.promotion_count, 0),
    %s
FROM keys k
LEFT JOIN sales s      ON s.product_id = k.product_id AND s.metric_date = k.metric_date
LEFT JOIN inventory inv ON inv.product_id = k.product_id AND inv.metric_date = k.metric_date
LEFT JOIN promos pm    ON pm.product_id = k.product_id AND pm.metric_date = k.metric_date
"""

# ---------- 3. gold.customer_daily: 1 baris per customer per hari ----------
# FULL OUTER JOIN order harian + tiket support harian: hari yang cuma ada kontak
# support (tanpa order) tetap muncul dengan order_count = 0.
CUSTOMER_DAILY = """
INSERT INTO gold.customer_daily (
    customer_id, metric_date, order_count, unit_quantity, gross_revenue,
    net_revenue, refund_amount, return_count, support_contacts, active_channel,
    new_customer_flag, repeat_customer_flag, pipeline_run_id
)
WITH orders AS (
    SELECT customer_id, order_date AS metric_date,
           COUNT(*) AS order_count,
           SUM(unit_quantity) FILTER (WHERE order_status <> 'CANCELLED') AS unit_quantity,
           SUM(gross_merchandise_value) FILTER (WHERE order_status <> 'CANCELLED') AS gross_revenue,
           SUM(net_revenue) AS net_revenue,
           SUM(refunded_amount) AS refund_amount,
           COUNT(*) FILTER (WHERE return_status <> 'NONE') AS return_count,   -- punya return event (aturan 29)
           MODE() WITHIN GROUP (ORDER BY sales_channel) AS active_channel,
           BOOL_OR(first_order_flag) AS has_first_order
    FROM gold.order_360
    GROUP BY customer_id, order_date
),
support AS (                                   -- tiket dibuka, tanggal pakai occurred_at (aturan 22, 30)
    SELECT customer_id, (occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date,
           COUNT(DISTINCT ticket_id) AS support_contacts
    FROM silver.support_events
    WHERE event_type = 'SUPPORT_TICKET_CREATED'
    GROUP BY customer_id, (occurred_at_utc AT TIME ZONE 'UTC')::date
)
SELECT
    COALESCE(o.customer_id, s.customer_id),
    COALESCE(o.metric_date, s.metric_date),
    COALESCE(o.order_count, 0),
    COALESCE(o.unit_quantity, 0),
    COALESCE(o.gross_revenue, 0),
    COALESCE(o.net_revenue, 0),
    COALESCE(o.refund_amount, 0),
    COALESCE(o.return_count, 0),
    COALESCE(s.support_contacts, 0),
    o.active_channel,
    COALESCE(o.has_first_order, FALSE),
    COALESCE(o.order_count > 0 AND NOT o.has_first_order, FALSE),
    %s
FROM orders o
FULL OUTER JOIN support s ON s.customer_id = o.customer_id AND s.metric_date = o.metric_date
"""

# ---------- 4. gold.channel_campaign_daily: 1 baris per hari x channel x campaign ----------
# 3 sumber (atribusi order, funnel session, spend iklan) dijumlah terpisah, lalu digabung.
# Baris tanpa spend tetap ada: campaign_spend = 0, roas = NULL (aturan 33).
CHANNEL_CAMPAIGN_DAILY = """
INSERT INTO gold.channel_campaign_daily (
    metric_date, sales_channel, campaign_id, campaign_spend, attributed_orders,
    attributed_customers, gross_revenue, net_revenue, refunds, roas,
    conversion_rate, pipeline_run_id
)
WITH attributed AS (                           -- event PURCHASE yang punya campaign (aturan 32, 33)
    SELECT o.order_date AS metric_date, o.sales_channel, w.campaign_id,
           COUNT(DISTINCT o.order_id) AS attributed_orders,
           COUNT(DISTINCT o.customer_id) AS attributed_customers,
           COALESCE(SUM(o.gross_merchandise_value) FILTER (WHERE o.order_status <> 'CANCELLED'), 0) AS gross_revenue,
           SUM(o.net_revenue) AS net_revenue,
           SUM(o.refunded_amount) AS refunds
    FROM silver.web_events w
    JOIN gold.order_360 o ON o.order_id = w.order_id
    WHERE w.event_type = 'PURCHASE' AND w.campaign_id IS NOT NULL
    GROUP BY o.order_date, o.sales_channel, w.campaign_id
),
sessions AS (                                  -- 1 baris per session (aturan 31)
    SELECT session_id, campaign_id, channel,
           (MIN(occurred_at_utc) AT TIME ZONE 'UTC')::date AS metric_date,
           BOOL_OR(event_type = 'PAGE_VIEW') AS viewed,
           BOOL_OR(event_type = 'PURCHASE') AS purchased
    FROM silver.web_events
    WHERE campaign_id IS NOT NULL
    GROUP BY session_id, campaign_id, channel
),
funnel AS (
    SELECT metric_date, channel AS sales_channel, campaign_id,
           COUNT(*) FILTER (WHERE viewed) AS viewed_sessions,
           COUNT(*) FILTER (WHERE purchased) AS purchased_sessions
    FROM sessions
    GROUP BY metric_date, channel, campaign_id
),
spend AS (
    SELECT spend_date AS metric_date, channel AS sales_channel, campaign_id,
           SUM(spend_amount) AS campaign_spend
    FROM silver.campaign_spend
    GROUP BY spend_date, channel, campaign_id
),
keys AS (
    SELECT metric_date, sales_channel, campaign_id FROM attributed
    UNION SELECT metric_date, sales_channel, campaign_id FROM funnel
    UNION SELECT metric_date, sales_channel, campaign_id FROM spend
)
SELECT
    k.metric_date,
    k.sales_channel,
    k.campaign_id,
    COALESCE(sp.campaign_spend, 0),
    COALESCE(a.attributed_orders, 0),
    COALESCE(a.attributed_customers, 0),
    COALESCE(a.gross_revenue, 0),
    COALESCE(a.net_revenue, 0),
    COALESCE(a.refunds, 0),
    COALESCE(a.net_revenue, 0) / NULLIF(sp.campaign_spend, 0),
    f.purchased_sessions::numeric / NULLIF(f.viewed_sessions, 0),
    %s
FROM keys k
LEFT JOIN attributed a ON a.metric_date = k.metric_date AND a.sales_channel = k.sales_channel AND a.campaign_id = k.campaign_id
LEFT JOIN funnel f     ON f.metric_date = k.metric_date AND f.sales_channel = k.sales_channel AND f.campaign_id = k.campaign_id
LEFT JOIN spend sp     ON sp.metric_date = k.metric_date AND sp.sales_channel = k.sales_channel AND sp.campaign_id = k.campaign_id
"""

# ---------- 5. gold.executive_kpis_daily: 1 baris per hari ----------
# Semua pembagian pakai NULLIF(..., 0) supaya tidak error bagi-nol,
# lalu COALESCE(..., 0) karena kolom kontrak NOT NULL.
EXECUTIVE_KPIS_DAILY = """
INSERT INTO gold.executive_kpis_daily (
    metric_date, total_orders, gross_revenue, net_revenue, average_order_value,
    refund_rate, return_rate, repeat_customer_rate, stockout_rate,
    active_customers, support_contact_rate, data_freshness_utc, pipeline_run_id
)
WITH daily AS (
    SELECT order_date AS metric_date,
           COUNT(*) AS total_orders,
           COALESCE(SUM(gross_merchandise_value) FILTER (WHERE order_status <> 'CANCELLED'), 0) AS gross_revenue,
           SUM(net_revenue) AS net_revenue,
           COUNT(*) FILTER (WHERE refunded_amount > 0) AS refunded_orders,
           COUNT(*) FILTER (WHERE captured_payment_amount > 0) AS captured_orders,
           COUNT(*) FILTER (WHERE return_status <> 'NONE') AS returned_orders,   -- punya return event (aturan 29)
           COUNT(DISTINCT customer_id) AS active_customers,
           COUNT(DISTINCT customer_id) FILTER (WHERE NOT first_order_flag) AS repeat_customers
    FROM gold.order_360
    GROUP BY order_date
),
stock AS (
    SELECT metric_date, AVG(stockout_flag::int) AS stockout_rate
    FROM gold.product_daily
    WHERE stockout_flag IS NOT NULL
    GROUP BY metric_date
),
tickets AS (
    SELECT (occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date, COUNT(DISTINCT ticket_id) AS tickets
    FROM silver.support_events
    WHERE event_type = 'SUPPORT_TICKET_CREATED'
    GROUP BY 1
),
freshness AS (
    SELECT GREATEST(
        (SELECT MAX(updated_at_utc) FROM silver.orders),
        (SELECT MAX(ingested_at_utc) FROM silver.payment_events),
        (SELECT MAX(ingested_at_utc) FROM silver.refund_events),
        (SELECT MAX(ingested_at_utc) FROM silver.return_events),
        (SELECT MAX(ingested_at_utc) FROM silver.support_events),
        (SELECT MAX(ingested_at_utc) FROM silver.web_events)
    ) AS data_freshness_utc
)
SELECT
    d.metric_date,
    d.total_orders,
    d.gross_revenue,
    d.net_revenue,
    COALESCE(d.net_revenue / NULLIF(d.total_orders, 0), 0),                    -- AOV per resolved order
    COALESCE(d.refunded_orders::numeric / NULLIF(d.captured_orders, 0), 0),
    COALESCE(d.returned_orders::numeric / NULLIF(d.total_orders, 0), 0),       -- return_rate (README bagian 4)
    COALESCE(d.repeat_customers::numeric / NULLIF(d.active_customers, 0), 0),
    COALESCE(st.stockout_rate, 0),
    d.active_customers,
    COALESCE(t.tickets::numeric / NULLIF(d.total_orders, 0), 0),
    fr.data_freshness_utc,
    %s
FROM daily d
CROSS JOIN freshness fr
LEFT JOIN stock st  ON st.metric_date = d.metric_date
LEFT JOIN tickets t ON t.metric_date = d.metric_date
"""


def build_gold(pipeline_run_id):
    """Isi ulang 5 tabel Gold dari Silver, lalu tampilkan jumlah barisnya."""
    with connect("local") as conn:
        conn.execute(
            "UPDATE ops.pipeline_runs SET current_stage = 'gold' WHERE pipeline_run_id = %s",
            (pipeline_run_id,),
        )
        conn.execute("TRUNCATE " + ", ".join(GOLD_TABLES))
        # Urutan penting: order_360 duluan karena 4 tabel lain membacanya;
        # product_daily sebelum executive_kpis_daily karena stockout_rate membacanya.
        # Satu-satunya parameter (%s) di setiap SQL = pipeline_run_id.
        for sql in (ORDER_360, PRODUCT_DAILY, CUSTOMER_DAILY, CHANNEL_CAMPAIGN_DAILY, EXECUTIVE_KPIS_DAILY):
            conn.execute(sql, (pipeline_run_id,))
        for table in GOLD_TABLES:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table:30} {count}")


# Rekonsiliasi. Setiap SQL mengembalikan selisih mutlak antara dua angka yang seharusnya sama
# (biasanya Gold vs Silver). Selisih di atas 0.01 = check gagal.
RECONCILIATION_CHECKS = [
    ("order_count_match", """
        SELECT ABS((SELECT COUNT(*) FROM gold.order_360) - (SELECT COUNT(*) FROM silver.orders))
    """),
    ("gmv_match", """
        SELECT ABS((SELECT SUM(gross_merchandise_value) FROM gold.order_360)
                 - (SELECT SUM(quantity * unit_price) FROM silver.order_items))
    """),
    ("captured_match", """
        SELECT ABS((SELECT SUM(captured_payment_amount) FROM gold.order_360)
                 - (SELECT SUM(amount) FROM silver.payment_events WHERE event_type = 'PAYMENT_CAPTURED'))
    """),
    ("refund_match", """
        SELECT ABS((SELECT SUM(refunded_amount) FROM gold.order_360)
                 - (SELECT COALESCE(SUM(r.amount), 0) FROM silver.refund_events r
                    WHERE r.event_type = 'REFUND_COMPLETED'
                      AND EXISTS (SELECT 1 FROM silver.payment_events p
                                  WHERE p.order_id = r.order_id AND p.event_type = 'PAYMENT_CAPTURED')))
    """),
    ("net_revenue_formula", """
        SELECT COUNT(*) FROM gold.order_360
        WHERE net_revenue <> CASE WHEN order_status = 'CANCELLED' THEN 0
                                  ELSE gross_merchandise_value - discount_amount
                                       + shipping_revenue - refunded_amount END
    """),
    ("executive_orders_match", """
        SELECT ABS((SELECT SUM(total_orders) FROM gold.executive_kpis_daily)
                 - (SELECT COUNT(*) FROM gold.order_360))
    """),
    ("executive_net_match", """
        SELECT ABS((SELECT SUM(net_revenue) FROM gold.executive_kpis_daily)
                 - (SELECT SUM(net_revenue) FROM gold.order_360))
    """),
    ("customer_net_match", """
        SELECT ABS((SELECT SUM(net_revenue) FROM gold.customer_daily)
                 - (SELECT SUM(net_revenue) FROM gold.order_360))
    """),
    # product_daily tidak memuat item PROD-MISSING (aturan 7): itu satu-satunya selisih yang sah.
    ("product_revenue_match", """
        SELECT ABS((SELECT SUM(gross_revenue) FROM gold.product_daily)
                 + (SELECT COALESCE(SUM(i.quantity * i.unit_price), 0)
                    FROM silver.order_items i JOIN silver.orders o ON o.order_id = i.order_id
                    WHERE NOT i.is_known_product AND o.status <> 'CANCELLED')
                 - (SELECT SUM(gross_revenue) FROM gold.executive_kpis_daily))
    """),
    ("campaign_spend_match", """
        SELECT ABS((SELECT SUM(campaign_spend) FROM gold.channel_campaign_daily)
                 - (SELECT SUM(spend_amount) FROM silver.campaign_spend))
    """),
    # Check referensi (seperti LEFT ANTI JOIN di lab W2D1): setiap ID di Gold harus dikenal
    # di tabel master Silver. Angka = jumlah baris Gold yang ID-nya tidak dikenal.
    ("order_customer_known", """
        SELECT COUNT(*) FROM gold.order_360 o
        WHERE NOT EXISTS (SELECT 1 FROM silver.customers c WHERE c.customer_id = o.customer_id)
    """),
    ("order_store_known", """
        SELECT COUNT(*) FROM gold.order_360 o
        WHERE o.store_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.stores s WHERE s.store_id = o.store_id)
    """),
    ("product_known", """
        SELECT COUNT(*) FROM gold.product_daily p
        WHERE NOT EXISTS (SELECT 1 FROM silver.products s WHERE s.product_id = p.product_id)
    """),
    ("campaign_channel_known", """
        SELECT COUNT(*) FROM gold.channel_campaign_daily g
        WHERE NOT EXISTS (SELECT 1 FROM silver.sales_channels s WHERE s.channel_id = g.sales_channel)
    """),
]


def validate_gold(pipeline_run_id):
    """Jalankan RECONCILIATION_CHECKS, simpan di ops; semua lolos -> run ditandai SUCCESS."""
    failed = []
    with connect("local") as conn:
        conn.execute(
            "UPDATE ops.pipeline_runs SET current_stage = 'validate_gold' WHERE pipeline_run_id = %s",
            (pipeline_run_id,),
        )
        for name, sql in RECONCILIATION_CHECKS:
            difference = conn.execute(sql).fetchone()[0] or 0
            passed = difference <= 0.01
            conn.execute(
                """
                INSERT INTO ops.quality_results (pipeline_run_id, check_name, failed_rows, passed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (pipeline_run_id, check_name) DO UPDATE
                SET failed_rows = EXCLUDED.failed_rows, passed = EXCLUDED.passed, checked_at_utc = now()
                """,
                (pipeline_run_id, name, difference, passed),
            )
            print(f"{'PASS' if passed else 'FAIL':5} {name:26} {difference}")
            if not passed:
                failed.append(name)
        if not failed:
            conn.execute(
                """
                UPDATE ops.pipeline_runs
                SET status = 'SUCCESS', current_stage = 'done', finished_at_utc = now()
                WHERE pipeline_run_id = %s
                """,
                (pipeline_run_id,),
            )
    if failed:
        raise ValueError(f"rekonsiliasi gold gagal: {', '.join(failed)}")


if __name__ == "__main__":
    build_gold("manual_test")
    validate_gold("manual_test")
