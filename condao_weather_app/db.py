# db.py —— 昆岛气象系统数据库模块（SQLite）
# 支持预报最低温/最高温、METAR 云量、阵风、雨型等

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


def init_db():
    """初始化数据库并创建数据表"""
    with get_conn() as conn:
        c = conn.cursor()

        # ------------------ 预报表：最低温 / 最高温 ------------------
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,             -- 预报日期
                wind TEXT,             -- 风向风速
                temp_min REAL,         -- 最低温
                temp_max REAL,         -- 最高温
                weather TEXT,          -- 天气现象
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------ METAR 解析结果表（完整字段） ------------------
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS metars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                obs_time TEXT,          -- 报文时间 (191200Z)
                station TEXT,           -- 站号如 VVCS

                raw TEXT,               -- 原始报文

                wind_dir TEXT,          -- 风向（270 / VRB）
                wind_speed REAL,        -- 风速 kt
                wind_gust REAL,         -- 阵风 kt

                visibility INTEGER,     -- 能见度 m

                temp REAL,              -- 温度 ℃
                dewpoint REAL,          -- 露点 ℃

                weather TEXT,           -- 中文天气现象（逗号拼接）
                rain_flag INTEGER,      -- 是否降水（1是0否）
                rain_level_cn TEXT,     -- 雨型 小雨/中雨/大雨/雷阵雨

                cloud_1_amount TEXT,    -- 第一层云 FEW/SCT/BKN/OVC
                cloud_1_height_m REAL,  -- 第一层云底高度（米）

                cloud_2_amount TEXT,
                cloud_2_height_m REAL,

                cloud_3_amount TEXT,
                cloud_3_height_m REAL,

                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ------------------ 降水事件表 ------------------
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS rain_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TEXT,        -- 降水开始时间 YYYY-MM-DD HH:MM
                rain_level_cn TEXT,     -- 雨强 小雨/中雨/大雨/雷阵雨
                rain_code TEXT,         -- 对应报文代码（如 -RA）
                note TEXT,              -- 备注
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.commit()


# -----------------------------------------------------------
#         预报数据（Forecast）
# -----------------------------------------------------------

def insert_forecast(date_str, wind, temp_min, temp_max, weather):
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


# -----------------------------------------------------------
#         METAR 解析记录（METAR）
# -----------------------------------------------------------

def insert_metar(record: dict):
    """将 parse_metar() 返回结果写入数据库"""

    station = record.get("station")
    obs_time = record.get("obs_time")
    raw = record.get("raw")

    wind_dir_raw = record.get("wind_direction")
    wind_dir = str(wind_dir_raw) if wind_dir_raw is not None else None

    wind_speed = record.get("wind_speed")
    wind_gust = record.get("wind_gust")

    visibility = record.get("visibility")

    temp = record.get("temperature")
    dewpoint = record.get("dewpoint")

    weather_list = record.get("weather") or []
    weather_text = ", ".join(weather_list) if weather_list else None

    is_raining = bool(record.get("is_raining"))
    rain_flag = 1 if is_raining else 0
    rain_level_cn = record.get("rain_type")

    clouds = record.get("clouds") or []

    def cloud(i):
        if i < len(clouds):
            return clouds[i].get("amount"), clouds[i].get("height_m")
        return None, None

    c1_amount, c1_height = cloud(0)
    c2_amount, c2_height = cloud(1)
    c3_amount, c3_height = cloud(2)

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
                wind_dir,
                wind_speed,
                wind_gust,
                visibility,
                temp,
                dewpoint,
                weather_text,
                rain_flag,
                rain_level_cn,
                c1_amount,
                c1_height,
                c2_amount,
                c2_height,
                c3_amount,
                c3_height,
            ),
        )

        conn.commit()


def get_recent_metars(limit=50):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT
                obs_time, station, raw,
                wind_dir, wind_speed, wind_gust,
                visibility,
                temp, dewpoint,
                weather, rain_flag, rain_level_cn,
                cloud_1_amount, cloud_1_height_m,
                cloud_2_amount, cloud_2_height_m,
                cloud_3_amount, cloud_3_height_m
            FROM metars
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return c.fetchall()


# -----------------------------------------------------------
#           降水事件记录（Rain Events）
# -----------------------------------------------------------

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
    with get_conn() as conn:
        c = conn.cursor()

        if start_date and end_date:
            c.execute(
                """
                SELECT date(start_time), COUNT(*)
                FROM rain_events
                WHERE date(start_time) BETWEEN ? AND ?
                GROUP BY date(start_time)
                ORDER BY date(start_time)
                """,
                (start_date, end_date),
            )
        else:
            c.execute(
                """
                SELECT date(start_time), COUNT(*)
                FROM rain_events
                GROUP BY date(start_time)
                ORDER BY date(start_time)
                """
            )

        return c.fetchall()
