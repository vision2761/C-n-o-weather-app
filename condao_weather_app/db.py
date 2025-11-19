# db.py
# SQLite 数据库封装（支持预报、METAR解析结果、降水记录）
import sqlite3
from contextlib import contextmanager

DB_NAME = "kunda.db"


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()


# ------------------ 昆岛预报表（升级版：最低温/最高温） ------------------

def init_db():
    with get_conn() as conn:
        c = conn.cursor()

        # 新版预报表（拆分最低温 / 最高温）
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,           -- 预报日期
                wind TEXT,           -- 风向风速
                temp_min REAL,       -- 最低温
                temp_max REAL,       -- 最高温
                weather TEXT,        -- 天气现象
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.commit()


def insert_forecast(date_str, wind, temp_min, temp_max, weather):
    """保存预报（最低温、最高温）"""
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO forecasts (date, wind, temp_min, temp_max, weather)
            VALUES (?, ?, ?, ?, ?)
            """,
            (date_str, wind, temp_min, temp_max, weather),
        )
        conn.commit()


def get_forecasts(start_date=None, end_date=None):
    """按日期范围查询预报"""
    with get_conn() as conn:
        c = conn.cursor()
        if start_date and end_date:
            c.execute(
                """
                SELECT date, wind, temp_min, temp_max, weather
                FROM forecasts
                WHERE date BETWEEN ? AND ?
                ORDER BY date
                """,
                (start_date, end_date),
            )
        else:
            c.execute(
                """
                SELECT date, wind, temp_min, temp_max, weather
                FROM forecasts
                ORDER BY date DESC
                LIMIT 50
                """
            )

        return c.fetchall()


# ------------ METAR 相关 ------------

def insert_metar(record: dict):
    """
    将 metar_parser.parse_metar 返回的解析结果写入数据库
    record 需要包含：
      - raw / station / obs_time
      - temperature / dewpoint
      - wind_direction / wind_speed / wind_gust
      - visibility
      - weather (中文描述列表)
      - is_raining (bool)
      - rain_type (中文雨型)
      - clouds: 最多三层，每层 {amount, height_m}
    """
    station = record.get("station")
    obs_time = record.get("obs_time")
    raw = record.get("raw")

    wind_dir = record.get("wind_direction")
    # 存文本：'270' 或 'None'
    wind_dir_str = None
    if wind_dir is not None:
        wind_dir_str = str(wind_dir)

    wind_speed = record.get("wind_speed")
    wind_gust = record.get("wind_gust")

    visibility = record.get("visibility")

    temp = record.get("temperature")
    dewpoint = record.get("dewpoint")

    weather_list = record.get("weather") or []
    weather_text = ", ".join(weather_list) if weather_list else None

    is_raining = bool(record.get("is_raining"))
    rain_flag = 1 if is_raining else 0
    rain_level_cn = record.get("rain_type")  # 小雨/中雨/大雨/雷阵雨

    clouds = record.get("clouds") or []
    # 取前三层云
    def get_cloud(i):
        if i < len(clouds):
            return clouds[i].get("amount"), clouds[i].get("height_m")
        return None, None

    c1_amount, c1_h = get_cloud(0)
    c2_amount, c2_h = get_cloud(1)
    c3_amount, c3_h = get_cloud(2)

    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO metars (
                obs_time, station, raw,
                wind_dir, wind_speed, wind_gust,
                visibility,
                temp, dewpoint,
                weather, rain_flag, rain_level_cn,
                cloud_1_amount, cloud_1_height_m,
                cloud_2_amount, cloud_2_height_m,
                cloud_3_amount, cloud_3_height_m
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                obs_time,
                station,
                raw,
                wind_dir_str,
                wind_speed,
                wind_gust,
                visibility,
                temp,
                dewpoint,
                weather_text,
                rain_flag,
                rain_level_cn,
                c1_amount,
                c1_h,
                c2_amount,
                c2_h,
                c3_amount,
                c3_h,
            ),
        )
        conn.commit()


def get_recent_metars(limit=50):
    """获取最近若干条 METAR 解析记录"""
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT
                obs_time,
                station,
                raw,
                wind_dir,
                wind_speed,
                wind_gust,
                visibility,
                temp,
                dewpoint,
                weather,
                rain_flag,
                rain_level_cn,
                cloud_1_amount,
                cloud_1_height_m,
                cloud_2_amount,
                cloud_2_height_m,
                cloud_3_amount,
                cloud_3_height_m
            FROM metars
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return c.fetchall()


# ------------ 降水记录相关 ------------

def insert_rain_event(start_time_str, rain_level_cn, rain_code, note):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO rain_events (start_time, rain_level_cn, rain_code, note)
            VALUES (?, ?, ?, ?)
            """,
            (start_time_str, rain_level_cn, rain_code, note),
        )
        conn.commit()


def get_rain_events(start_date=None, end_date=None):
    with get_conn() as conn:
        c = conn.cursor()
        if start_date and end_date:
            c.execute(
                """
                SELECT start_time, rain_level_cn, rain_code, note
                FROM rain_events
                WHERE date(start_time) BETWEEN ? AND ?
                ORDER BY start_time
                """,
                (start_date, end_date),
            )
        else:
            c.execute(
                """
                SELECT start_time, rain_level_cn, rain_code, note
                FROM rain_events
                ORDER BY start_time DESC
                LIMIT 100
                """
            )
        return c.fetchall()


def get_rain_stats_by_day(start_date=None, end_date=None):
    """统计每天降水记录条数"""
    with get_conn() as conn:
        c = conn.cursor()
        if start_date and end_date:
            c.execute(
                """
                SELECT date(start_time) as d, COUNT(*) as cnt
                FROM rain_events
                WHERE date(start_time) BETWEEN ? AND ?
                GROUP BY date(start_time)
                ORDER BY d
                """,
                (start_date, end_date),
            )
        else:
            c.execute(
                """
                SELECT date(start_time) as d, COUNT(*) as cnt
                FROM rain_events
                GROUP BY date(start_time)
                ORDER BY d
                """
            )
        return c.fetchall()
