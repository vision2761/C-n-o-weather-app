# metar_parser.py
# 昆岛（Côn Đảo）及周边机场 METAR 报文解析模块
# 结合你之前的 ConDaoWeatherSystem 解析逻辑，并增加云量(ft→m)解析

import re
from datetime import datetime
from typing import Dict, List, Optional


def _parse_temp_pair(metar_text: str):
    """解析温度/露点对，格式: 28/24 或 M02/M05"""
    temp_match = re.search(r'\b(M?\d{2})/(M?\d{2})\b', metar_text)
    if not temp_match:
        return None, None

    temp_str = temp_match.group(1)
    dew_str = temp_match.group(2)

    def to_value(s: str) -> int:
        if s.startswith("M"):
            return -int(s[1:])
        return int(s)

    temperature = to_value(temp_str)
    dewpoint = to_value(dew_str)
    return temperature, dewpoint


def _parse_wind(metar_text: str):
    """
    解析风:
      - 27015KT
      - 27015G25KT
      - VRB02KT
    """
    wind_match = re.search(r'(\d{3}|VRB)(\d{2,3})(?:G(\d{2,3}))?KT', metar_text)
    if not wind_match:
        return None, None, None

    dir_str = wind_match.group(1)
    speed_str = wind_match.group(2)
    gust_str = wind_match.group(3)

    wind_direction = None if dir_str == "VRB" else int(dir_str)
    wind_speed = int(speed_str)
    wind_gust = int(gust_str) if gust_str else None

    return wind_direction, wind_speed, wind_gust


def _parse_visibility(metar_text: str):
    """解析能见度，简化为第一个独立的4位数字"""
    vis_match = re.search(r'\b(\d{4})\b', metar_text)
    if not vis_match:
        return None
    return int(vis_match.group(1))


def _parse_station_and_time(metar_text: str):
    """
    解析站号（VVCS/VVTS/VVNB 等）和观测时间（6位+Z）
    支持：
      - METAR VVCS 191200Z ...
      - SPECI VVTS 210530Z ...
      - VVNB 210600Z ...
    """
    tokens = metar_text.strip().split()
    station = None
    obs_time = None

    # 找 station
    for i, tok in enumerate(tokens):
        if tok in ("METAR", "SPECI"):
            # 下一个若是 4 字母则认为是站号
            if i + 1 < len(tokens) and re.match(r'^[A-Z]{4}$', tokens[i + 1]):
                station = tokens[i + 1]
            continue

    if station is None:
        # 未显式带 METAR/SPECI，则第一个 4 字母 token 作为站号
        for tok in tokens:
            if re.match(r'^[A-Z]{4}$', tok):
                station = tok
                break

    # 找观测时间（191200Z 这种）
    for tok in tokens:
        if re.match(r'^\d{6}Z$', tok):
            obs_time = tok
            break

    return station, obs_time


def _parse_clouds(metar_text: str) -> List[Dict]:
    """
    解析云量与云底高度:
      FEW020 SCT025 BKN015 OVC010
    转换为:
      amount: FEW/SCT/BKN/OVC
      height_ft: 2000
      height_m: 610
    最多返回 3 层云
    """
    clouds = []
    # 匹配 FEW020 / SCT025 / BKN015 / OVC010
    for amount, h_str in re.findall(r'\b(FEW|SCT|BKN|OVC)(\d{3})\b', metar_text):
        height_code = int(h_str)        # 020 -> 20 (hundreds of feet)
        height_ft = height_code * 100   # 20 * 100 = 2000ft
        height_m = round(height_ft * 0.3048)  # 转换为米
        clouds.append(
            {
                "amount": amount,
                "height_ft": height_ft,
                "height_m": height_m,
            }
        )
        if len(clouds) >= 3:
            break

    return clouds


# 天气现象与雨型逻辑 —— 完全沿用你之前的规则
WEATHER_PATTERNS = {
    r'\+RA': ('大雨', True, '大雨'),
    r'\-RA': ('小雨', True, '小雨'),
    r'\bRA\b': ('中雨', True, '中雨'),
    r'\+SHRA': ('大阵雨', True, '大雨'),
    r'\-SHRA': ('小阵雨', True, '小雨'),
    r'\bSHRA\b': ('中阵雨', True, '中雨'),
    r'TSRA': ('雷雨', True, '雷阵雨'),
    r'\bTS\b': ('雷暴', False, None),
    r'\bDZ\b': ('毛毛雨', True, '小雨'),
    r'\bFG\b': ('雾', False, None),
    r'\bBR\b': ('薄雾', False, None),
    r'\bHZ\b': ('霾', False, None),
}


def _parse_weather_and_rain(metar_text: str):
    """
    解析天气现象（中文描述列表）、是否下雨、雨型（小雨/中雨/大雨/雷阵雨）
    完全保持你原来的逻辑
    """
    weather_desc = []
    is_raining = False
    rain_type = None

    for pattern, (description, is_rain, r_type) in WEATHER_PATTERNS.items():
        if re.search(pattern, metar_text):
            weather_desc.append(description)
            if is_rain:
                is_raining = True
                # 使用第一个匹配到的雨型作为主雨型
                if rain_type is None and r_type is not None:
                    rain_type = r_type

    return weather_desc, is_raining, rain_type


def parse_metar(metar_text: str) -> Dict:
    """
    面向你的 Web 小程序使用的统一解析接口
    返回字典字段包括：
      - raw: 原始报文
      - timestamp: 解析时间（ISO）
      - station: 站号（VVCS 等）
      - obs_time: 报文时间（例如 191200Z）
      - temperature: 温度（℃）
      - dewpoint: 露点（℃）
      - wind_direction: 风向（°，可为 None）
      - wind_speed: 风速（kt）
      - wind_gust: 阵风（kt）
      - visibility: 能见度（m）
      - weather: 中文天气描述列表
      - is_raining: 是否在下雨
      - rain_type: 雨型（小雨/中雨/大雨/雷阵雨）
      - clouds: 最多三层云，每层含 amount / height_ft / height_m
    """
    text = metar_text.strip().upper()

    station, obs_time = _parse_station_and_time(text)
    temperature, dewpoint = _parse_temp_pair(text)
    wind_direction, wind_speed, wind_gust = _parse_wind(text)
    visibility = _parse_visibility(text)
    clouds = _parse_clouds(text)
    weather_desc, is_raining, rain_type = _parse_weather_and_rain(text)

    result = {
        "raw": metar_text.strip(),  # 保留原始大小写
        "timestamp": datetime.now().isoformat(),

        "station": station,
        "obs_time": obs_time,

        "temperature": temperature,
        "dewpoint": dewpoint,

        "wind_direction": wind_direction,
        "wind_speed": wind_speed,
        "wind_gust": wind_gust,

        "visibility": visibility,

        "weather": weather_desc,   # 中文描述列表
        "is_raining": is_raining,
        "rain_type": rain_type,    # 小雨/中雨/大雨/雷阵雨

        "clouds": clouds,          # 最多三层云
    }

    return result
