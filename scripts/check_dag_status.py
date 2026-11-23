import psycopg2
from datetime import datetime

conn = psycopg2.connect(
    host='127.0.0.1',
    port=5433,
    user='postgres',
    password='postgres',
    dbname='airflow_metadata'
)
cur = conn.cursor()

# Mark cancelled run as failed if still marked running
cur.execute("""
    UPDATE dag_run
    SET state = 'failed', end_date = NOW()
    WHERE dag_id = 'ecommerce_data_engineering_pipeline'
      AND run_id = 'manual__2026-09-05T09:53:35+00:00'
      AND state = 'running';
""")

cur.execute("""
    UPDATE task_instance
    SET state = 'failed', end_date = NOW()
    WHERE dag_id = 'ecommerce_data_engineering_pipeline'
      AND run_id = 'manual__2026-09-05T09:53:35+00:00'
      AND state = 'running';
""")
conn.commit()

print("================================================================================")
print("AIRFLOW DAG RUNS SUMMARY")
print("================================================================================")
cur.execute("""
    SELECT run_id, state, execution_date, start_date, end_date
    FROM dag_run
    WHERE dag_id = 'ecommerce_data_engineering_pipeline'
    ORDER BY execution_date ASC;
""")
runs = cur.fetchall()
for r in runs:
    print(f"Run ID        : {r[0]}")
    print(f"State         : {r[1]}")
    print(f"Execution Date: {r[2]}")
    print(f"Start Date    : {r[3]}")
    print(f"End Date      : {r[4]}")
    duration = (r[4] - r[3]).total_seconds() if r[3] and r[4] else None
    print(f"Duration      : {duration} seconds\n")

print("================================================================================")
print("FIRST DAG RUN (COMPLETED RUN): manual__2026-09-05T08:57:17+00:00")
print("================================================================================")
cur.execute("""
    SELECT task_id, state, start_date, end_date, duration
    FROM task_instance
    WHERE dag_id = 'ecommerce_data_engineering_pipeline'
      AND run_id = 'manual__2026-09-05T08:57:17+00:00'
    ORDER BY start_date ASC;
""")
first_run_tasks = cur.fetchall()
for t in first_run_tasks:
    print(f"Task: {t[0]:<30} | State: {t[1]:<10} | Start: {t[2]} | End: {t[3]} | Duration: {t[4]:.2f}s" if t[4] is not None else f"Task: {t[0]:<30} | State: {t[1]:<10}")

print("\n================================================================================")
print("SECOND DAG RUN (CANCELLED RUN): manual__2026-09-05T09:53:35+00:00")
print("================================================================================")
cur.execute("""
    SELECT task_id, state, start_date, end_date, duration
    FROM task_instance
    WHERE dag_id = 'ecommerce_data_engineering_pipeline'
      AND run_id = 'manual__2026-09-05T09:53:35+00:00'
    ORDER BY start_date ASC;
""")
second_run_tasks = cur.fetchall()
for t in second_run_tasks:
    task_name = t[0] or "unknown"
    state_str = str(t[1]) if t[1] is not None else "None"
    if t[4] is not None:
        print(f"Task: {task_name:<30} | State: {state_str:<10} | Start: {t[2]} | End: {t[3]} | Duration: {t[4]:.2f}s")
    else:
        print(f"Task: {task_name:<30} | State: {state_str:<10} | Start: {t[2]} | End: {t[3]}")

conn.close()
