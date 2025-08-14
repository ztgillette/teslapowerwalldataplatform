# ingest_live_data.py
import os, time, signal, sys, datetime
from dotenv import load_dotenv
from teslapy import Tesla
import snowflake.connector

load_dotenv()

TESLA_EMAIL = os.getenv("TESLA_EMAIL")
TESLA_CACHE = os.path.expanduser(os.getenv("TESLA_CACHE", "/cache/tesla_token.json"))
INTERVAL_S  = int(os.getenv("INTERVAL_SECONDS", "60"))

SF_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER      = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SF_ROLE      = os.getenv("SNOWFLAKE_ROLE")
SF_WH        = os.getenv("SNOWFLAKE_WAREHOUSE", "CLIMATE_WH")
SF_DB        = os.getenv("SNOWFLAKE_DATABASE", "TESLA_DATA")
SF_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")

_running = True
def _stop(*_): 
    global _running; _running = False
signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)

def get_live_tesla_data():
    with Tesla(TESLA_EMAIL, cache_file=TESLA_CACHE) as tesla:
        if tesla.authorized and tesla.token.get("refresh_token"):
            tesla.refresh_token()
        else:
            raise RuntimeError(
                "Can't find Tesla Cache"
            )



        products = tesla.get('api/1/products')['response']
        sites = [p for p in products if p.get('energy_site_id')]
        if not sites:
            raise RuntimeError("Can't find energy site")
        site_id = sites[0]['energy_site_id']

        live = tesla.get(f'api/1/energy_sites/{site_id}/live_status')['response']
        return {
            "ts": live.get("timestamp"),
            "solar_w": live.get("solar_power"),
            "load_w": live.get("load_power"),
            "grid_w": live.get("grid_power"),
            "battery_w": live.get("battery_power"),
            "battery_soc": live.get("percentage_charged"),
            "grid_status": live.get("grid_status"),
            "island_status": live.get("island_status"),
        }

def insert_into_snowflake(data):
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        role=SF_ROLE, warehouse=SF_WH, database=SF_DB, schema=SF_SCHEMA
    )
    cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {SF_DB}.{SF_SCHEMA}.LIVE_DATA
            (TS, SOLAR_W, LOAD_W, GRID_W, BATTERY_W, BATTERY_SOC, GRID_STATUS, ISLAND_STATUS)
            VALUES (
                CAST(TO_TIMESTAMP_TZ(%(ts)s) AS TIMESTAMP_NTZ),
                %(solar_w)s, %(load_w)s, %(grid_w)s, %(battery_w)s, %(battery_soc)s, %(grid_status)s, %(island_status)s
            )
        """, data)

        conn.commit()
    finally:
        cur.close(); conn.close()

if __name__ == "__main__":
    print("[poller] starting…", flush=True)
    while _running:
        try:
            rec = get_live_tesla_data()
            insert_into_snowflake(rec)
            print(f"[{datetime.datetime.now().isoformat()}] OK {rec['ts']}", flush=True)
        except Exception as e:
            print("[poller] ERROR:", e, flush=True)
        
        end = time.time() + INTERVAL_S
        while _running and time.time() < end:
            time.sleep(1)
    print("[poller] stopped.", flush=True)
