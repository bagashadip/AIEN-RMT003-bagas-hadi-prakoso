import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Supaya folder `pipelines` dan `shared` bisa di-import oleh Airflow.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.bronze.build_bronze import init_schemas, load_bronze, validate_raw_files  # noqa: E402
from pipelines.gold.build_gold import build_gold, validate_gold  # noqa: E402
from pipelines.silver.build_silver import build_silver, run_quality_gate  # noqa: E402
from shared.db import apply_sql_file, connect  # noqa: E402


# ---------- Callable tiap task ----------
# Airflow mengirim "context" (info run) sebagai keyword argument; run_id ada di context["run_id"].

def task_validate_raw_files(**context):
    # Schema ops dibuat duluan karena hasil validasi (sidik jari file) dicatat di ops.raw_files.
    # Aman diulang: ops.sql memakai IF NOT EXISTS.
    with connect("local") as conn:
        apply_sql_file(conn, PROJECT_ROOT / "database" / "schemas" / "ops.sql")
    validate_raw_files(context["run_id"])


def task_init_schemas(**context):
    init_schemas()


def task_load_bronze(**context):
    load_bronze(context["run_id"])


def task_build_silver(**context):
    build_silver(context["run_id"])


def task_quality_gate(**context):
    run_quality_gate(context["run_id"])


def task_build_gold(**context):
    build_gold(context["run_id"])


def task_validate_gold(**context):
    validate_gold(context["run_id"])


def mark_run_failed(context):
    """on_failure_callback: catat status FAILED di ops.pipeline_runs.

    Pakai INSERT ... ON CONFLICT karena kalau gagal di tahap paling awal, baris run
    mungkin belum ada (insert di validate_raw_files ikut ter-rollback).
    """
    run_id = context["run_id"]
    task_id = context["task_instance"].task_id
    exception = context.get("exception")
    # Saat dijalankan lewat `airflow dags test`, exception kadang tidak dikirim ke callback.
    error = str(exception)[:2000] if exception else f"Task {task_id} gagal, detail ada di log Airflow"
    try:
        with connect("local") as conn:
            conn.execute(
                """
                INSERT INTO ops.pipeline_runs
                    (pipeline_run_id, status, current_stage, finished_at_utc, error_message)
                VALUES (%s, 'FAILED', %s, now(), %s)
                ON CONFLICT (pipeline_run_id) DO UPDATE
                SET status = 'FAILED', current_stage = EXCLUDED.current_stage,
                    finished_at_utc = now(), error_message = EXCLUDED.error_message
                """,
                (run_id, task_id, error),
            )
    except Exception as callback_error:  # callback tidak boleh menutupi error aslinya
        print(f"Gagal mencatat status FAILED di ops: {callback_error}")


# ---------- Definisi DAG ----------

with DAG(
    dag_id="omnichannel_retail_pipeline",
    description="Bronze -> Silver -> quality gate -> Gold -> validasi, target PostgreSQL lokal",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Jakarta"),
    schedule=None,          # dijalankan manual (Trigger DAG); snapshot data lake tidak berubah harian
    catchup=False,
    max_active_runs=1,      # jangan ada 2 run bersamaan yang sama-sama TRUNCATE Silver/Gold
    default_args={
        "owner": "peserta",
        "retries": 1,                       # coba ulang sekali untuk gangguan sementara (mis. koneksi DB)
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": mark_run_failed,
    },
    tags=["milestone1", "retail", "bronze-silver-gold"],
) as dag:
    validate = PythonOperator(task_id="validate_raw_files", python_callable=task_validate_raw_files)
    init = PythonOperator(task_id="init_schemas", python_callable=task_init_schemas)
    bronze = PythonOperator(task_id="load_bronze", python_callable=task_load_bronze)
    silver = PythonOperator(task_id="build_silver", python_callable=task_build_silver)
    gate = PythonOperator(task_id="quality_gate", python_callable=task_quality_gate)
    gold = PythonOperator(task_id="build_gold", python_callable=task_build_gold)
    final = PythonOperator(task_id="validate_gold", python_callable=task_validate_gold)

    validate >> init >> bronze >> silver >> gate >> gold >> final
