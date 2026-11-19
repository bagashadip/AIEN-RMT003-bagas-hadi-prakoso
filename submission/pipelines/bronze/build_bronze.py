import csv
import hashlib
import json
from pathlib import Path

from shared.db import apply_sql_file, connect

# Root project = naik 2 folder dari file ini (bronze -> pipelines -> root).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"

FILES = [
    "operational/customers.json",
    "operational/customer_addresses.json",
    "operational/customer_profiles.json",
    "operational/orders.json",
    "operational/order_items.json",
    "operational/order_promotions.json",
    "operational/products.json",
    "operational/product_categories.json",
    "operational/promotions.json",
    "operational/stores.json",
    "operational/sales_channels.json",
    "events/payment_events.json",
    "events/refund_events.json",
    "events/return_events.json",
    "events/support_events.json",
    "events/web_events.json",
    "inventory/inventory_snapshots.csv",
    "reference/campaign_spend.csv",
    "reference/city_reference.json"
]

EVENT_FIELDS = {"event_id", "event_type", "occurred_at_utc", "ingested_at_utc", "payload"}
REQUIRED_FIELDS = {
    "operational/customers.json": {"customer_id", "city_id", "customer_segment", "created_at_utc"},
    "operational/customer_addresses.json": {"address_id", "customer_id", "city_id", "valid_from_utc", "valid_to_utc"},
    "operational/customer_profiles.json": {"source_row_id", "customer_id", "city_id", "customer_segment",
                                           "valid_from_utc", "valid_to_utc"},
    "operational/orders.json": {"order_id", "customer_id", "store_id", "sales_channel", "status",
                                "ordered_at_utc", "updated_at_utc", "shipping_revenue", "source_row_id"},
    "operational/order_items.json": {"order_item_id", "order_id", "product_id", "quantity", "unit_price",
                                     "item_discount_amount"},
    "operational/order_promotions.json": {"source_row_id", "order_id", "promotion_id", "discount_amount"},
    "operational/products.json": {"product_id", "product_name", "sku", "unit_price"},
    "operational/product_categories.json": {"source_row_id", "product_id", "category_id", "category_name",
                                            "valid_from_utc", "valid_to_utc"},
    "operational/promotions.json": {"promotion_id", "promotion_code", "promotion_type", "discount_rate",
                                    "start_date", "end_date"},
    "operational/stores.json": {"store_id", "store_name", "city_id", "location_type"},
    "operational/sales_channels.json": {"channel_id", "channel_name"},
    "events/payment_events.json": EVENT_FIELDS,
    "events/refund_events.json": EVENT_FIELDS,
    "events/return_events.json": EVENT_FIELDS,
    "events/support_events.json": EVENT_FIELDS,
    "events/web_events.json": EVENT_FIELDS,
    "inventory/inventory_snapshots.csv": {"product_id", "location_id", "snapshot_date", "available_quantity",
                                          "reserved_quantity", "unit_cost"},
    "reference/campaign_spend.csv": {"spend_date", "campaign_id", "channel", "spend_amount"},
    "reference/city_reference.json": {"city_id", "city_name", "country"},
}


def check_structure(file, records):
    """Pastikan setiap record berbentuk object dan punya semua field wajib. Gagal -> raise."""
    required = REQUIRED_FIELDS[file]
    for row_number, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"Struktur {file} salah: record ke-{row_number} bukan object")
        missing_fields = required - set(record)
        if missing_fields:
            raise ValueError(
                f"Struktur {file} tidak lengkap: record ke-{row_number} tanpa field {sorted(missing_fields)}"
            )


def read_records(path):
    """Baca 1 file jadi list of dict (1 dict = 1 record), untuk JSON maupun CSV."""
    with open(path, encoding="utf-8") as f:
        if path.suffix == ".json":
            # File JSON berisi array of objects -> langsung jadi list of dict.
            return json.load(f)
        elif path.suffix == ".csv":
            # DictReader memakai baris header sebagai key. Semua nilai terbaca sebagai teks.
            # Dibungkus list() agar dibaca semua sebelum file ditutup.
            return list(csv.DictReader(f))
        else:
            raise ValueError(f"Unsupported file type: {path}")


def validate_raw_files(pipeline_run_id):
    """Tahap 1: validasi file mentah dan catat hasilnya di schema ops."""
    # Kumpulkan dulu semua file yang hilang, supaya pesan error tampil semuanya.
    missing = []
    for file in FILES:
        path = RAW / file
        if not path.exists():
            missing.append(file)

    if missing:
        raise FileNotFoundError(f"File hilang: {missing}")

    with connect("local") as conn:
        conn.execute(
            """
            INSERT INTO ops.pipeline_runs (pipeline_run_id, status, current_stage)
            VALUES (%s, 'RUNNING', 'validate')
            ON CONFLICT (pipeline_run_id) DO UPDATE
            SET status = 'RUNNING', current_stage = 'validate',
                error_message = NULL, finished_at_utc = NULL
            """,
            (pipeline_run_id,),
        )

        for f in FILES:
            path = RAW / f
            records = read_records(path)
            if len(records) == 0:
                raise ValueError(f"File kosong: {f}")
            check_structure(f, records)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()

            conn.execute(
                """
                INSERT INTO ops.raw_files (pipeline_run_id, file_path, sha256, record_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (pipeline_run_id, file_path) DO UPDATE
                SET sha256 = EXCLUDED.sha256,
                    record_count = EXCLUDED.record_count
                """,
                (pipeline_run_id, f, sha, len(records))
            )

    print(f"{len(FILES)} file valid")


def init_schemas():
    """Tahap 2: jalankan file DDL. Aman diulang karena semua pakai IF NOT EXISTS."""
    with connect("local") as conn:
        for name in ["ops.sql", "bronze.sql", "silver.sql", "gold.sql"]:
            apply_sql_file(conn, PROJECT_ROOT / "database" / "schemas" / name)
    print("schema siap")


def load_bronze(pipeline_run_id):
    """Tahap 3: muat setiap file ke bronze.raw_records (append-only & idempotent)."""
    with connect("local") as conn:
        for f in FILES:
            path = RAW / f
            records = read_records(path)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()

            conn.execute(
                """
                INSERT INTO bronze.raw_records
                    (source_file, source_row_number, source_record_id, payload, file_sha256, pipeline_run_id)
                SELECT %s, e.ord, COALESCE(e.value->>'source_row_id', e.value->>'event_id'), e.value, %s, %s
                FROM jsonb_array_elements(%s::jsonb) WITH ORDINALITY AS e(value, ord)
                ON CONFLICT (source_file, source_row_number, file_sha256) DO NOTHING
                """,
                (f, sha, pipeline_run_id, json.dumps(records))
            )
            print(f"bronze: {f} ({len(records)} record)")

        conn.execute(
            """
            UPDATE ops.pipeline_runs
            SET current_stage = 'bronze'
            WHERE pipeline_run_id = %s
            """,
            (pipeline_run_id,)
        )



if __name__ == "__main__":
    run_id = "manual_test"
    validate_raw_files(run_id)
    init_schemas()
    load_bronze(run_id)
