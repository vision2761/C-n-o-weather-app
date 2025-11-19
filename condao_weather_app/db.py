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


def init_db():
    """初始化数据库和数据表"""
    with get_conn() as conn:
        c = conn.cursor()

        # 昆岛天气预报表
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,            -- 预报日期 YYYY-MM-DD
                wind TEXT,            -- 风向风速文字描述
                temp REAL,            -- 气温(℃)
                weather TEXT,         -- 天气现象
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # METAR/SPECI 报文解析结果表（升级版）
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS metars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                obs_time TEXT,        -- 报文时间(例如 191200Z)
                station TEXT,         -- 站号，如 VVCS / VVTS
                raw TEXT,             -- 原始报文全文

                wind_dir TEXT,        -- 风向（度数或 VRB，文本形式）
                wind_speed REAL,      -- 风速(kt)
                wind_gust REAL,       -- 阵风(kt)

                visibility INTEGER,   -- 能见度(m)

                temp REAL,            -- 温度(℃)
                dewpoint REAL,        -- 露点(℃)

                weather TEXT,         -- 中文天气描述（逗号拼接）
                rain_flag INTEGER,    -- 是否降水(1是0否)
                rain_level_cn TEXT,   -- 雨型：小雨/中雨/大雨/雷阵雨

                cloud_1_amount TEXT,  -- 第一层云量 FEW/SCT/BKN/OVC
                cloud_1_height_m REAL,-- 第一层云底高度(米)
                cloud_2_amount TEXT,
                cloud_2_height_m REAL,
                cloud_3_amount TEXT,
                cloud_3_height_m REAL,

                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # 降水记录表（保留原设计）
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS rain_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TEXT,      -- 降水开始时间 YYYY-MM-DD HH:MM:SS
                rain_level_cn TEXT,   -- 雨强(中文)：小雨/中雨/大雨/雷阵雨等
                rain_code TEXT,       -- 报文中的降水代码：-RA / RA / +RA / TSRA 等
                note TEXT,            -- 备注
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.commit()


# ------------ 昆岛预报相关 ------------

def insert_forecast(date_str, wind, temp, weather):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO forecasts (date, wind, temp, weather) VALUES (?, ?, ?, ?)",
            (date_str, wind, temp, weather),
        )
        conn.commit()


def get_forecasts(start_date=None, end_date=None):
    """按日期范围查询预报，日期格式 YYYY-MM-DD"""
    with get_conn() as conn:
        c = conn.cursor()
        if start_date and end_date:
            c.execute(
                """
                SELECT date, wind, temp, weather
                FROM forecasts
                WHERE date BETWEEN ? AND ?
                ORDER BY date
                """,
                (start_date, end_date),
            )
        else:
            c.execute(
                """
                SELECT date, wind, temp, weather
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
