from shared.db import connect

SILVER_TABLES = [
    "silver.rejected_records",
    "silver.sales_channels",
    "silver.stores",
    "silver.cities",
    "silver.products",
    "silver.promotions",
    "silver.customers",
    "silver.customer_profiles",
    "silver.customer_addresses",
    "silver.product_categories",
    "silver.orders",
    "silver.order_items",
    "silver.order_promotions",
    "silver.payment_events",
    "silver.refund_events",
    "silver.return_events",
    "silver.support_events",
    "silver.web_events",
    "silver.inventory_snapshots",
    "silver.campaign_spend",
]

SRC = """
    SELECT r.bronze_id, r.source_file, r.source_record_id, r.payload AS p, f.pipeline_run_id
    FROM bronze.raw_records r
    JOIN ops.raw_files f ON f.file_path = r.source_file AND f.sha256 = r.file_sha256
    WHERE f.pipeline_run_id = %s AND r.source_file = %s
"""

LINEAGE_COLS = "bronze_id, source_file, source_record_id, p, pipeline_run_id"


def field(name):
    """Field di level atas record: p->>'name' (selalu teks)."""
    return f"p->>'{name}'"


def payload(name):
    """Field event ada di dalam objek "payload": p->'payload'->>'name'."""
    return f"p->'payload'->>'{name}'"


def bad(expr, sql_type):
    """Kondisi 'wajib ada tapi kosong / tidak bisa diparse' (pengganti TRY_CAST IS NULL di lab).

    pg_input_is_valid('abc', 'numeric') -> false, jadi baris ditolak, bukan pipeline crash.
    """
    return f"({expr} IS NULL OR NOT pg_input_is_valid({expr}, '{sql_type}'))"


def bad_optional(expr, sql_type):
    """Seperti bad(), tapi boleh kosong: hanya gagal kalau ADA isinya dan tidak bisa diparse."""
    return f"({expr} IS NOT NULL AND NOT pg_input_is_valid({expr}, '{sql_type}'))"


def silver_step(table, file, columns, key, rules, duplicate_reason, order_by="bronze_id"):
    """Bikin SQL untuk 1 tabel Silver dengan pola validasi -> dedup -> tolak/simpan.

    table            : nama tabel tujuan, mis. "silver.orders"
    file             : file sumber di Bronze
    columns          : list (nama_kolom_silver, ekspresi_SQL) yang dimasukkan
    key              : ekspresi kunci bisnis untuk dedup (PARTITION BY)
    rules            : list (kondisi_SQL, alasan) dicek berurutan; yang pertama kena dipakai
    duplicate_reason : alasan untuk versi yang kalah saat dedup
    order_by         : urutan pemenang dedup (baris pertama = yang disimpan)
    """
    case_rules = "\n                ".join(f"WHEN {condition} THEN '{reason}'" for condition, reason in rules)
    names = ", ".join(name for name, _ in columns)
    exprs = ",\n            ".join(expr for _, expr in columns)
    return (file, f"""
        WITH src AS ({SRC}),
        validated AS (
            SELECT s.*, CASE
                {case_rules}
            END AS invalid_reason
            FROM src s
        ),
        valid_ranked AS (
            SELECT v.*, ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY {order_by}) AS rn
            FROM validated v
            WHERE invalid_reason IS NULL
        ),
        checked AS (
            SELECT {LINEAGE_COLS}, invalid_reason AS reason
            FROM validated
            WHERE invalid_reason IS NOT NULL
            UNION ALL
            SELECT {LINEAGE_COLS}, CASE WHEN rn > 1 THEN '{duplicate_reason}' END
            FROM valid_ranked
        ),
        rejected AS (
            INSERT INTO silver.rejected_records
                (source_file, source_record_id, bronze_id, reason, payload, pipeline_run_id)
            SELECT source_file, source_record_id, bronze_id, reason, p, pipeline_run_id
            FROM checked
            WHERE reason IS NOT NULL
        )
        INSERT INTO {table} ({names}, bronze_id, pipeline_run_id)
        SELECT
            {exprs},
            bronze_id, pipeline_run_id
        FROM checked
        WHERE reason IS NULL
    """)


def event_step(table, file, extra_columns, amount_rule=False):
    """Tabel event: kolom dasar sama untuk 5 file event, kunci dedup = event_id."""
    columns = [
        ("event_id", field("event_id")),
        ("event_type", field("event_type")),
        ("occurred_at_utc", f"({field('occurred_at_utc')})::timestamptz"),  # +07:00 otomatis jadi UTC (aturan 19)
        ("ingested_at_utc", f"({field('ingested_at_utc')})::timestamptz"),
    ] + extra_columns
    rules = [
        (f"{field('event_id')} IS NULL", "invalid_key"),
        (bad(field("occurred_at_utc"), "timestamptz"), "invalid_timestamp"),
        (bad(field("ingested_at_utc"), "timestamptz"), "invalid_timestamp"),
    ]
    if amount_rule:
        rules += [
            (bad(payload("amount"), "numeric"), "invalid_amount"),
            (f"({payload('amount')})::numeric <= 0", "invalid_amount"),
        ]
    rules.append((
        f"{payload('order_id')} IS NOT NULL AND NOT EXISTS "
        f"(SELECT 1 FROM silver.orders o WHERE o.order_id = {payload('order_id')})",
        "orphan_order",
    ))
    return silver_step(table, file, columns, key=field("event_id"), rules=rules,
                       duplicate_reason="duplicate_event_id")


def versioned_columns(*names):
    """Kolom teks biasa + valid_from_utc / valid_to_utc bertipe timestamptz."""
    return [(n, field(n)) for n in names] + [
        ("valid_from_utc", f"({field('valid_from_utc')})::timestamptz"),
        ("valid_to_utc", f"({field('valid_to_utc')})::timestamptz"),
    ]


VERSION_RULES = [
    (bad(field("valid_from_utc"), "timestamptz"), "invalid_timestamp"),
    (bad(field("valid_to_utc"), "timestamptz"), "invalid_timestamp"),
]

STATUS_LIST = "'PLACED', 'CONFIRMED', 'FULFILLED', 'CANCELLED', 'RETURNED'"

STEPS = [
    silver_step(
        "silver.sales_channels", "operational/sales_channels.json",
        [("channel_id", field("channel_id")), ("channel_name", field("channel_name"))],
        key=field("channel_id"),
        rules=[(f"{field('channel_id')} IS NULL", "invalid_key")],
        duplicate_reason="duplicate_channel_id",
    ),
    silver_step(
        "silver.stores", "operational/stores.json",
        [(n, field(n)) for n in ("store_id", "store_name", "city_id", "location_type")],
        key=field("store_id"),
        rules=[(f"{field('store_id')} IS NULL", "invalid_key")],
        duplicate_reason="duplicate_store_id",
    ),
    silver_step(
        "silver.cities", "reference/city_reference.json",
        [(n, field(n)) for n in ("city_id", "city_name", "country")],
        key=field("city_id"),
        rules=[(f"{field('city_id')} IS NULL", "invalid_key")],
        duplicate_reason="duplicate_city_id",
    ),
    silver_step(
        "silver.products", "operational/products.json",
        [(n, field(n)) for n in ("product_id", "product_name", "sku")]
        + [("unit_price", f"({field('unit_price')})::numeric")],
        key=field("product_id"),
        rules=[
            (f"{field('product_id')} IS NULL", "invalid_key"),
            (bad(field("unit_price"), "numeric"), "invalid_price"),
            (f"({field('unit_price')})::numeric < 0", "invalid_price"),
        ],
        duplicate_reason="duplicate_product_id",
    ),
    silver_step(
        "silver.promotions", "operational/promotions.json",
        [(n, field(n)) for n in ("promotion_id", "promotion_code", "promotion_type")] + [
            ("discount_rate", f"({field('discount_rate')})::numeric"),
            ("start_date", f"({field('start_date')})::date"),
            ("end_date", f"({field('end_date')})::date"),
        ],
        key=field("promotion_id"),
        rules=[
            (f"{field('promotion_id')} IS NULL", "invalid_key"),
            (bad_optional(field("discount_rate"), "numeric"), "invalid_amount"),
            (bad(field("start_date"), "date"), "invalid_date"),
            (bad(field("end_date"), "date"), "invalid_date"),
        ],
        duplicate_reason="duplicate_promotion_id",
    ),
    silver_step(
        "silver.customers", "operational/customers.json",
        [(n, field(n)) for n in ("customer_id", "city_id", "customer_segment")]
        + [("created_at_utc", f"({field('created_at_utc')})::timestamptz")],
        key=field("customer_id"),
        rules=[
            (f"{field('customer_id')} IS NULL", "invalid_key"),
            (bad_optional(field("created_at_utc"), "timestamptz"), "invalid_timestamp"),
        ],
        duplicate_reason="duplicate_customer_id",
    ),
    silver_step(  # berversi: satu customer bisa punya beberapa profil
        "silver.customer_profiles", "operational/customer_profiles.json",
        versioned_columns("source_row_id", "customer_id", "city_id", "customer_segment"),
        key=field("source_row_id"),
        rules=[(f"{field('source_row_id')} IS NULL", "invalid_key")] + VERSION_RULES,
        duplicate_reason="duplicate_profile_row",
    ),
    silver_step(
        "silver.customer_addresses", "operational/customer_addresses.json",
        versioned_columns("address_id", "customer_id", "city_id"),
        key=field("address_id"),
        rules=[(f"{field('address_id')} IS NULL", "invalid_key")] + VERSION_RULES,
        duplicate_reason="duplicate_address_id",
    ),
    silver_step(  # berversi: kategori produk bisa berubah
        "silver.product_categories", "operational/product_categories.json",
        versioned_columns("source_row_id", "product_id", "category_id", "category_name"),
        key=field("source_row_id"),
        rules=[(f"{field('source_row_id')} IS NULL", "invalid_key")] + VERSION_RULES,
        duplicate_reason="duplicate_category_row",
    ),

    # Dedup di antara order VALID: updated_at_utc terbaru menang; kalau seri, source_row_id terkecil.
    silver_step(
        "silver.orders", "operational/orders.json",
        [(n, field(n)) for n in ("order_id", "customer_id", "store_id", "sales_channel", "status")] + [
            ("ordered_at_utc", f"({field('ordered_at_utc')})::timestamptz"),
            ("updated_at_utc", f"({field('updated_at_utc')})::timestamptz"),
            ("shipping_revenue", f"({field('shipping_revenue')})::numeric"),
            ("source_row_id", field("source_row_id")),
        ],
        key=field("order_id"),
        order_by=f"({field('updated_at_utc')})::timestamptz DESC, {field('source_row_id')} ASC",
        rules=[
            (f"{field('order_id')} IS NULL", "invalid_key"),
            (f"{field('customer_id')} IS NULL", "invalid_customer"),
            (bad(field("ordered_at_utc"), "timestamptz"), "invalid_timestamp"),
            (bad(field("updated_at_utc"), "timestamptz"), "invalid_timestamp"),
            (bad(field("shipping_revenue"), "numeric"), "invalid_amount"),
            (f"({field('shipping_revenue')})::numeric < 0", "invalid_amount"),
            (f"{field('status')} IS NULL OR {field('status')} NOT IN ({STATUS_LIST})", "invalid_status"),
            (f"NOT EXISTS (SELECT 1 FROM silver.sales_channels c WHERE c.channel_id = {field('sales_channel')})",
             "invalid_channel"),
            (f"{field('sales_channel')} = 'STORE' AND {field('store_id')} IS NULL", "missing_store"),
            (f"({field('updated_at_utc')})::timestamptz < ({field('ordered_at_utc')})::timestamptz",
             "updated_before_ordered"),
        ],
        duplicate_reason="duplicate_order_id",
    ),

    # PROD-MISSING tidak ditolak, tapi ditandai is_known_product = false (aturan 7).
    silver_step(
        "silver.order_items", "operational/order_items.json",
        [(n, field(n)) for n in ("order_item_id", "order_id", "product_id")] + [
            ("quantity", f"({field('quantity')})::integer"),
            ("unit_price", f"({field('unit_price')})::numeric"),
            ("item_discount_amount", f"({field('item_discount_amount')})::numeric"),
            ("is_known_product",
             f"EXISTS (SELECT 1 FROM silver.products pr WHERE pr.product_id = {field('product_id')})"),
            ("source_row_id", field("source_row_id")),
        ],
        key=field("order_item_id"),
        rules=[
            (f"{field('order_item_id')} IS NULL", "invalid_key"),
            (bad(field("quantity"), "integer"), "invalid_quantity"),
            (f"({field('quantity')})::integer <= 0", "invalid_quantity"),
            (bad(field("unit_price"), "numeric"), "invalid_price"),
            (f"({field('unit_price')})::numeric < 0", "invalid_price"),
            (bad(field("item_discount_amount"), "numeric"), "invalid_amount"),
            (f"({field('item_discount_amount')})::numeric < 0", "invalid_amount"),
            (f"NOT EXISTS (SELECT 1 FROM silver.orders o WHERE o.order_id = {field('order_id')})", "orphan_item"),
        ],
        duplicate_reason="duplicate_order_item_id",
    ),

    # ---------- Order promotions (aturan 15) ----------
    silver_step(
        "silver.order_promotions", "operational/order_promotions.json",
        [(n, field(n)) for n in ("source_row_id", "order_id", "promotion_id")]
        + [("discount_amount", f"({field('discount_amount')})::numeric")],
        key=field("source_row_id"),
        rules=[
            (f"{field('source_row_id')} IS NULL", "invalid_key"),
            (bad(field("discount_amount"), "numeric"), "invalid_amount"),
            (f"({field('discount_amount')})::numeric < 0", "invalid_amount"),
            (f"NOT EXISTS (SELECT 1 FROM silver.orders o WHERE o.order_id = {field('order_id')})",
             "orphan_order_promotion"),
            (f"NOT EXISTS (SELECT 1 FROM silver.promotions pm WHERE pm.promotion_id = {field('promotion_id')})",
             "invalid_promotion"),
        ],
        duplicate_reason="duplicate_order_promotion",
    ),

    # ---------- Event (aturan 17-24) ----------
    event_step("silver.payment_events", "events/payment_events.json", [
        ("payment_id", payload("payment_id")),
        ("order_id", payload("order_id")),
        ("amount", f"({payload('amount')})::numeric"),
    ], amount_rule=True),
    event_step("silver.refund_events", "events/refund_events.json", [
        ("refund_id", payload("refund_id")),
        ("order_id", payload("order_id")),
        ("amount", f"({payload('amount')})::numeric"),
    ], amount_rule=True),
    event_step("silver.return_events", "events/return_events.json", [
        ("return_id", payload("return_id")),
        ("order_id", payload("order_id")),
    ]),
    event_step("silver.support_events", "events/support_events.json", [
        ("ticket_id", payload("ticket_id")),
        ("order_id", payload("order_id")),
        ("customer_id", payload("customer_id")),
        ("reason", payload("reason")),
    ]),
    event_step("silver.web_events", "events/web_events.json", [
        ("session_id", payload("session_id")),
        ("order_id", payload("order_id")),
        ("customer_id", payload("customer_id")),
        ("channel", payload("channel")),
        ("campaign_id", payload("campaign_id")),
    ]),

    # ---------- File CSV: semua nilai datang sebagai teks ----------
    silver_step(
        "silver.inventory_snapshots", "inventory/inventory_snapshots.csv",
        [("product_id", field("product_id")), ("location_id", field("location_id")),
         ("snapshot_date", f"({field('snapshot_date')})::date"),
         ("available_quantity", f"({field('available_quantity')})::integer"),
         ("reserved_quantity", f"({field('reserved_quantity')})::integer"),
         ("unit_cost", f"({field('unit_cost')})::numeric")],
        key=f"{field('product_id')}, {field('location_id')}, {field('snapshot_date')}",
        rules=[
            (f"{field('product_id')} IS NULL OR {field('location_id')} IS NULL", "invalid_key"),
            (bad(field("snapshot_date"), "date"), "invalid_date"),
            (bad(field("available_quantity"), "integer"), "invalid_quantity"),
            (bad(field("reserved_quantity"), "integer"), "invalid_quantity"),
            (bad(field("unit_cost"), "numeric"), "invalid_price"),
            (f"NOT EXISTS (SELECT 1 FROM silver.products pr WHERE pr.product_id = {field('product_id')})",
             "unknown_product"),
        ],
        duplicate_reason="duplicate_inventory_snapshot",
    ),
    silver_step(
        "silver.campaign_spend", "reference/campaign_spend.csv",
        [("spend_date", f"({field('spend_date')})::date"), ("campaign_id", field("campaign_id")),
         ("channel", field("channel")), ("spend_amount", f"({field('spend_amount')})::numeric")],
        key=f"{field('spend_date')}, {field('campaign_id')}, {field('channel')}",
        rules=[
            (f"{field('campaign_id')} IS NULL OR {field('channel')} IS NULL", "invalid_key"),
            (bad(field("spend_date"), "date"), "invalid_date"),
            (bad(field("spend_amount"), "numeric"), "invalid_amount"),
            (f"({field('spend_amount')})::numeric < 0", "invalid_amount"),
        ],
        duplicate_reason="duplicate_campaign_spend",
    ),
]


def build_silver(pipeline_run_id):
    """Isi ulang 20 tabel Silver dari Bronze, lalu tampilkan jumlah baris & penolakan."""
    with connect("local") as conn:
        # Kosongkan semua tabel dulu. RESTART IDENTITY = nomor rejected_id mulai lagi dari 1.
        conn.execute("TRUNCATE " + ", ".join(SILVER_TABLES) + " RESTART IDENTITY")
        # Jalankan tiap INSERT berurutan. 2 parameter = isian %s di potongan SRC.
        for file, sql in STEPS:
            conn.execute(sql, (pipeline_run_id, file))
        conn.execute(
            "UPDATE ops.pipeline_runs SET current_stage = 'silver' WHERE pipeline_run_id = %s",
            (pipeline_run_id,),
        )

        # Ringkasan untuk dicek manual.
        for table in SILVER_TABLES:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table:32} {count}")
        print("rejected by reason:")
        for reason, count in conn.execute(
            "SELECT reason, COUNT(*) FROM silver.rejected_records GROUP BY 1 ORDER BY 1"
        ).fetchall():
            print(f"  {reason:30} {count}")


# Quality gate. Setiap SQL mengembalikan SATU angka: jumlah baris yang melanggar aturan.
# Check wajib (required = True) harus 0, kalau tidak pipeline berhenti sebelum Gold.
# Check peringatan (required = False) = masalah data sumber yang sudah diputuskan cara
# menanganinya (aturan 27 dan 29): tetap dicatat di ops, tapi tidak menghentikan pipeline.
QUALITY_CHECKS = [
    # (nama_check, wajib?, sql)
    ("duplicate_orders", True, """
        SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM silver.orders
    """),
    ("orphan_items", True, """
        SELECT COUNT(*) FROM silver.order_items i
        WHERE NOT EXISTS (SELECT 1 FROM silver.orders o WHERE o.order_id = i.order_id)
    """),
    ("non_positive_quantity", True, """
        SELECT COUNT(*) FROM silver.order_items WHERE quantity <= 0
    """),
    ("updated_before_ordered", True, """
        SELECT COUNT(*) FROM silver.orders WHERE updated_at_utc < ordered_at_utc
    """),
    ("duplicate_payment_events", True, """
        SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM silver.payment_events
    """),
    ("non_positive_payment_amount", True, """
        SELECT COUNT(*) FROM silver.payment_events WHERE amount <= 0
    """),
    ("category_version_gap", True, """
        SELECT COUNT(*) FROM (
            SELECT i.order_item_id
            FROM silver.order_items i
            JOIN silver.orders o ON o.order_id = i.order_id
            LEFT JOIN silver.product_categories c
                   ON c.product_id = i.product_id
                  AND o.ordered_at_utc BETWEEN c.valid_from_utc AND c.valid_to_utc
            WHERE i.is_known_product
            GROUP BY i.order_item_id
            HAVING COUNT(c.source_row_id) <> 1
        ) gap
    """),
    ("refund_without_capture", False, """
        SELECT COUNT(DISTINCT r.order_id) FROM silver.refund_events r
        WHERE r.event_type = 'REFUND_COMPLETED'
          AND NOT EXISTS (SELECT 1 FROM silver.payment_events p
                          WHERE p.order_id = r.order_id AND p.event_type = 'PAYMENT_CAPTURED')
    """),
    ("return_status_mismatch", False, """
        SELECT
            (SELECT COUNT(DISTINCT r.order_id) FROM silver.return_events r
             JOIN silver.orders o ON o.order_id = r.order_id
             WHERE o.status <> 'RETURNED')
          + (SELECT COUNT(*) FROM silver.orders o
             WHERE o.status = 'RETURNED'
               AND NOT EXISTS (SELECT 1 FROM silver.return_events r WHERE r.order_id = o.order_id))
    """),
]


def run_quality_gate(pipeline_run_id):
    """Jalankan semua QUALITY_CHECKS, simpan hasilnya di ops, raise kalau check wajib gagal."""
    failed = []
    with connect("local") as conn:
        conn.execute(
            "UPDATE ops.pipeline_runs SET current_stage = 'quality_gate' WHERE pipeline_run_id = %s",
            (pipeline_run_id,),
        )
        for name, required, sql in QUALITY_CHECKS:
            bad_rows = conn.execute(sql).fetchone()[0]
            # Check peringatan selalu dianggap lolos; angkanya tetap disimpan.
            passed = bad_rows == 0 or not required
            conn.execute(
                """
                INSERT INTO ops.quality_results (pipeline_run_id, check_name, failed_rows, passed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (pipeline_run_id, check_name) DO UPDATE
                SET failed_rows = EXCLUDED.failed_rows, passed = EXCLUDED.passed, checked_at_utc = now()
                """,
                (pipeline_run_id, name, bad_rows, passed),
            )
            label = "PASS" if passed else "FAIL"
            if not required:
                label += " (warning)"
            print(f"{label:16} {name:30} {bad_rows}")
            if not passed:
                failed.append(name)
    # Raise SETELAH blok `with` selesai, supaya hasil check di atas sudah ter-commit
    # dan tetap kelihatan di ops.quality_results walaupun gate-nya gagal.
    if failed:
        raise ValueError(f"quality gate gagal: {', '.join(failed)}")


if __name__ == "__main__":
    build_silver("manual_test")
    run_quality_gate("manual_test")
