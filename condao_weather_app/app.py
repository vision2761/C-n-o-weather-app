# app.py —— 昆岛（Côn Đảo）气象记录与分析系统
# 适配：最低温 / 最高温，METAR 云量、雨型、阵风解析

import streamlit as st
import pandas as pd
import re
from datetime import datetime, time

from db import (
    init_db,
    insert_forecast,
    get_forecasts,
    insert_metar,
    get_recent_metars,
    insert_rain_event,
    get_rain_events,
    get_rain_stats_by_day,
)
from metar_parser import parse_metar

# 初始化数据库
init_db()

st.set_page_config(page_title="昆岛机场气象记录系统", layout="wide")

# -------------------------------------------------------------
# 页面：昆岛天气预报
# -------------------------------------------------------------
def page_forecast():
    st.header("📋 昆岛天气预报录入与查询")

    st.subheader("录入天气预报（支持最低温 / 最高温）")

    col1, col2 = st.columns(2)
    with col1:
        date_val = st.date_input("预报日期")
        wind = st.text_input("风向风速（示例：东南风3级 或 09005KT）")
    with col2:
        temp_range = st.text_input("气温范围（格式示例：25-28 或 24~30）")
        weather = st.text_input("天气现象（例：RA、SHRA、TSRA 等）")

    if st.button("保存预报记录"):
        parts = re.split(r"[-~]", temp_range)

        if len(parts) != 2:
            st.warning("气温范围格式不正确，应为 25-28 或 24~30")
        else:
            try:
                tmin = float(parts[0])
                tmax = float(parts[1])

                insert_forecast(str(date_val), wind, tmin, tmax, weather)
                st.success("✅ 天气预报已保存")
            except:
                st.error("气温必须为数字，例如 25-28")

    st.markdown("---")
    st.subheader("历史预报查询")

    c1, c2 = st.columns(2)
    with c1:
        start = st.date_input("开始日期", key="fc_start")
    with c2:
        end = st.date_input("结束日期", key="fc_end")

    if st.button("查询历史预报"):
        rows = get_forecasts(str(start), str(end))

        if not rows:
            st.info("此时间段无预报记录。")
        else:
            df = pd.DataFrame(
                rows,
                columns=["日期", "风向风速", "最低温(℃)", "最高温(℃)", "天气现象"],
            )
            st.dataframe(df, use_container_width=True)

            # 绘制平均气温折线图
            try:
                df["日期"] = pd.to_datetime(df["日期"])
                df["平均气温"] = (df["最低温(℃)"] + df["最高温(℃)"]) / 2
                df_chart = df.set_index("日期")

                if len(df_chart) > 1:
                    st.line_chart(df_chart["平均气温"], height=300)
                    st.caption("（图中显示的是气温范围的平均值）")

            except Exception as e:
                st.warning(f"图表渲染失败：{e}")


# -------------------------------------------------------------
# 页面：METAR 报文解析
# -------------------------------------------------------------
def page_metar():
    st.header("🛬 METAR/SPECI 报文解析")

    st.subheader("输入报文进行自动解析")
    raw = st.text_area(
        "示例：VVCS 201200Z 27015G25KT 4000 +SHRA TS SCT018 BKN030 OVC100 27/24 Q1008",
        height=120,
    )

    if st.button("解析并保存"):
        if not raw.strip():
            st.warning("请先输入报文")
        else:
            record = parse_metar(raw)
            insert_metar(record)

            st.success("✅ 报文已解析并保存")
            st.subheader("解析结果")
            st.json(record)

    st.markdown("---")
    st.subheader("📑 最近 METAR 解析记录")

    rows = get_recent_metars(limit=100)

    if not rows:
        st.info("暂无 METAR 数据")
        return

    df = pd.DataFrame(
        rows,
        columns=[
            "报文时间",
            "站号",
            "原始报文",
            "风向(°)",
            "风速(kt)",
            "阵风(kt)",
            "能见度(m)",
            "温度(℃)",
            "露点(℃)",
            "天气(中文)",
            "是否雨(1是0否)",
            "雨型",
            "云1量",
            "云1高(m)",
            "云2量",
            "云2高(m)",
            "云3量",
            "云3高(m)",
        ],
    )

    st.dataframe(df, use_container_width=True)

    rain_count = df["是否雨(1是0否)"].sum()
    st.caption(f"📌 最近记录中共有 **{rain_count} 条 METAR 含降水**。")


# -------------------------------------------------------------
# 页面：降水记录
# -------------------------------------------------------------
def page_rain():
    st.header("🌧 降水事件记录")

    st.subheader("记录一次降水开始时间")

    col1, col2, col3 = st.columns(3)
    with col1:
        d = st.date_input("开始日期")
    with col2:
        t = st.time_input("开始时间", value=time(0, 0))
    with col3:
        rain_level = st.selectbox("雨强（中文）", ["小雨", "中雨", "大雨", "雷阵雨"])

    rain_code = st.text_input("对应报文代码（如 -RA、RA、+RA、TSRA）")
    note = st.text_input("备注（可选）")

    if st.button("保存降水记录"):
        start_dt = datetime.combine(d, t).strftime("%Y-%m-%d %H:%M:%S")

        insert_rain_event(start_dt, rain_level, rain_code, note)
        st.success("🌧 降水记录已保存")

    st.markdown("---")
    st.subheader("历史降水查询")

    c1, c2 = st.columns(2)
    with c1:
        start = st.date_input("开始日期", key="rain_start")
    with c2:
        end = st.date_input("结束日期", key="rain_end")

    if st.button("查询降水历史"):
        rows = get_rain_events(str(start), str(end))

        if not rows:
            st.info("此时间段无降水记录")
        else:
            df = pd.DataFrame(
                rows,
                columns=["开始时间", "雨强", "报文代码", "备注"],
            )
            st.dataframe(df, use_container_width=True)

            stats = get_rain_stats_by_day(str(start), str(end))
            if stats:
                s_df = pd.DataFrame(stats, columns=["日期", "次数"])
                s_df["日期"] = pd.to_datetime(s_df["日期"])
                s_df = s_df.set_index("日期")

                st.bar_chart(s_df, y="次数", height=280)
                st.caption(f"📌 共记录 {s_df['次数'].sum()} 次降水事件。")


# -------------------------------------------------------------
# 页面：历史分析
# -------------------------------------------------------------
def page_analysis():
    st.header("📊 降水统计分析")

    st.subheader("按日统计降水次数")

    col1, col2 = st.columns(2)
    with col1:
        start = st.date_input("统计开始日期", key="ana_start")
    with col2:
        end = st.date_input("统计结束日期", key="ana_end")

    if st.button("生成统计图"):
        stats = get_rain_stats_by_day(str(start), str(end))

        if not stats:
            st.info("此时间段无降水记录")
            return

        df = pd.DataFrame(stats, columns=["日期", "次数"])
        df["日期"] = pd.to_datetime(df["日期"])
        df = df.set_index("日期")

        st.bar_chart(df, height=350)
        st.dataframe(df.reset_index(), use_container_width=True)

        st.caption(f"📌 统计天数：{len(df)} 天，共降水 {df['次数'].sum()} 次。")


# -------------------------------------------------------------
# 主菜单
# -------------------------------------------------------------
def main():
    st.title("✈ 昆岛机场（Côn Đảo）气象记录与分析系统")

    page = st.sidebar.radio(
        "功能选择",
        ["昆岛天气预报", "METAR 报文解析", "降水记录", "历史分析"],
    )

    if page == "昆岛天气预报":
        page_forecast()
    elif page == "METAR 报文解析":
        page_metar()
    elif page == "降水记录":
        page_rain()
    elif page == "历史分析":
        page_analysis()


if __name__ == "__main__":
    main()
