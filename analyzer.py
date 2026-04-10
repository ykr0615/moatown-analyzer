"""부동산 데이터 분석 모듈"""
import pandas as pd
import numpy as np


def analyze_by_gu(trade_df: pd.DataFrame) -> pd.DataFrame:
    """구별 매매 시세 분석"""
    if trade_df.empty:
        return pd.DataFrame()

    summary = trade_df.groupby("구").agg(
        평균매매가=("거래금액", "mean"),
        중위매매가=("거래금액", "median"),
        최고가=("거래금액", "max"),
        최저가=("거래금액", "min"),
        거래건수=("거래금액", "count"),
        평균평당가격=("평당가격", "mean"),
    ).round(0)

    summary = summary.sort_values("평균매매가", ascending=False)
    return summary


def analyze_price_trend(trade_df: pd.DataFrame, gu: str = None) -> pd.DataFrame:
    """월별 가격 추세 분석"""
    if trade_df.empty:
        return pd.DataFrame()

    df = trade_df.copy()
    df["거래년월"] = df["거래년도"] + "-" + df["거래월"].str.zfill(2)

    if gu:
        df = df[df["구"] == gu]

    trend = df.groupby("거래년월").agg(
        평균매매가=("거래금액", "mean"),
        중위매매가=("거래금액", "median"),
        거래건수=("거래금액", "count"),
        평균평당가격=("평당가격", "mean"),
    ).round(0)

    trend = trend.sort_index()
    return trend


def calculate_jeonse_ratio(trade_df: pd.DataFrame, rent_df: pd.DataFrame) -> pd.DataFrame:
    """전세가율 계산 (전세가 / 매매가)

    전세가율이 높을수록 → 갭투자에 유리 (적은 자본으로 투자 가능)
    전세가율이 낮을수록 → 매매가 대비 전세가가 낮아 갭이 큼
    """
    if trade_df.empty or rent_df.empty:
        return pd.DataFrame()

    # 전세만 필터링 (월세 = 0)
    jeonse_df = rent_df[rent_df["월세"] == 0].copy()

    # 구별 평균 매매가
    avg_trade = trade_df.groupby("구")["거래금액"].median().reset_index()
    avg_trade.columns = ["구", "매매중위가"]

    # 구별 평균 전세가
    avg_jeonse = jeonse_df.groupby("구")["보증금"].median().reset_index()
    avg_jeonse.columns = ["구", "전세중위가"]

    # 병합
    ratio_df = pd.merge(avg_trade, avg_jeonse, on="구", how="inner")
    ratio_df["전세가율"] = (ratio_df["전세중위가"] / ratio_df["매매중위가"] * 100).round(1)
    ratio_df["갭(매매-전세)"] = ratio_df["매매중위가"] - ratio_df["전세중위가"]
    ratio_df = ratio_df.sort_values("전세가율", ascending=False)

    return ratio_df


def find_investment_candidates(
    trade_df: pd.DataFrame,
    rent_df: pd.DataFrame,
    budget: int,
    strategy: str = "gap"
) -> pd.DataFrame:
    """투자 후보 매물 탐색

    Args:
        trade_df: 매매 실거래가 데이터
        rent_df: 전월세 실거래가 데이터
        budget: 투자 가능 금액 (만원 단위)
        strategy: 투자 전략
            - "gap": 갭투자 (매매가 - 전세가 기준)
            - "direct": 직접 매수 (총 매매가 기준)
    """
    if trade_df.empty:
        return pd.DataFrame()

    if strategy == "direct":
        # 예산 이내 매물
        candidates = trade_df[trade_df["거래금액"] <= budget].copy()
        candidates = candidates.sort_values("평당가격", ascending=True)
        return candidates

    # 갭투자 전략
    jeonse_df = rent_df[rent_df["월세"] == 0].copy() if not rent_df.empty else pd.DataFrame()
    if jeonse_df.empty:
        return pd.DataFrame()

    # 아파트+구 기준으로 매매/전세 매칭
    trade_avg = trade_df.groupby(["구", "매물명"]).agg(
        매매중위가=("거래금액", "median"),
        평균평당가격=("평당가격", "mean"),
        평균면적=("전용면적", "mean"),
        매매건수=("거래금액", "count"),
    ).reset_index()

    jeonse_avg = jeonse_df.groupby(["구", "매물명"]).agg(
        전세중위가=("보증금", "median"),
        전세건수=("보증금", "count"),
    ).reset_index()

    merged = pd.merge(trade_avg, jeonse_avg, on=["구", "매물명"], how="inner")
    merged["갭"] = merged["매매중위가"] - merged["전세중위가"]
    merged["전세가율"] = (merged["전세중위가"] / merged["매매중위가"] * 100).round(1)

    # 예산 이내 갭투자 가능 매물
    candidates = merged[merged["갭"] <= budget].copy()
    candidates = candidates[candidates["갭"] > 0]  # 역전세 제외
    candidates = candidates.sort_values("전세가율", ascending=False)
    candidates = candidates.round(0)

    return candidates


def get_market_summary(trade_df: pd.DataFrame, rent_df: pd.DataFrame) -> dict:
    """시장 전체 요약 통계"""
    summary = {
        "총거래건수": len(trade_df),
        "평균매매가": int(trade_df["거래금액"].mean()) if not trade_df.empty else 0,
        "중위매매가": int(trade_df["거래금액"].median()) if not trade_df.empty else 0,
        "평균평당가격": int(trade_df["평당가격"].mean()) if not trade_df.empty else 0,
    }

    if not rent_df.empty:
        jeonse = rent_df[rent_df["월세"] == 0]
        summary["전세거래건수"] = len(jeonse)
        summary["평균전세가"] = int(jeonse["보증금"].mean()) if not jeonse.empty else 0

    return summary
