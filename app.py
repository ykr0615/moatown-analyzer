"""서울 부동산 투자 AI 어드바이저 - Streamlit 대시보드"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from data_collector import (
    collect_seoul_trades, collect_seoul_rents,
    check_api_available, PROPERTY_TYPES,
)
from naver_crawler import (
    crawl_dong_listings, crawl_gu_listings,
    REAL_ESTATE_TYPES, SEOUL_GU_CORTAR,
)
from kb_crawler import (
    crawl_area_kb, get_dong_complexes,
    SEOUL_GU_CODES_KB, PROPERTY_TYPE_MAP,
)
from zigbang_crawler import crawl_listings as zigbang_crawl
from building_collector import collect_all_seoul, analyze_dong, SEOUL_DONG_CODES
from moatown_scorer import MoatownScorer, DESIGNATED_MOATOWNS
from dong_analyzer import diagnose_dong, DONG_CODE_MAP, ADMIN_TO_LEGAL
from analyzer import (
    analyze_by_gu,
    analyze_price_trend,
    calculate_jeonse_ratio,
    find_investment_candidates,
    get_market_summary,
)
from ai_advisor import generate_investment_report, ask_advisor
from config import SEOUL_GU_CODES
from datetime import datetime
from pathlib import Path
import os

# .env 로드
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, v = line.strip().split("=", 1)
                os.environ[k] = v

CURRENT_YEAR = datetime.now().year

st.set_page_config(
    page_title="서울 부동산 투자 AI 어드바이저",
    page_icon="🏠",
    layout="wide",
)

st.title("🏠 서울 부동산 투자 AI 어드바이저")
st.caption("국토교통부 실거래가 + 네이버 부동산 현재 매물 | AI 투자 분석")


# --- 사이드바 ---
with st.sidebar:
    st.header("⚙️ 설정")

    # 매물 타입 선택
    st.subheader("매물 타입")
    selected_types = st.multiselect(
        "분석할 매물 타입 선택",
        options=PROPERTY_TYPES,
        default=["아파트"],
        help="여러 타입을 동시에 선택하면 함께 분석됩니다",
    )

    # API 상태 표시
    if "api_status" not in st.session_state:
        st.session_state["api_status"] = {}

    if st.button("🔍 API 상태 확인", use_container_width=True):
        with st.spinner("API 확인 중..."):
            for ptype in PROPERTY_TYPES:
                st.session_state["api_status"][ptype] = check_api_available(ptype)

    if st.session_state["api_status"]:
        for ptype, available in st.session_state["api_status"].items():
            if available:
                st.caption(f"✅ {ptype}")
            else:
                st.caption(f"❌ {ptype} (data.go.kr에서 신청 필요)")

    st.divider()

    budget = st.number_input(
        "투자 가용 자산 (만원)",
        min_value=1000,
        max_value=500000,
        value=30000,
        step=5000,
        help="갭투자 시: 매매가-전세가 기준 / 직접매수 시: 총 매매가 기준",
    )
    st.caption(f"= {budget/10000:.1f}억원")

    strategy = st.selectbox(
        "투자 전략",
        options=["gap", "direct"],
        format_func=lambda x: "갭투자 (전세 끼고 매수)" if x == "gap" else "직접 매수",
    )

    months = st.slider("데이터 수집 기간 (개월)", 1, 12, 3)

    selected_gus = st.multiselect(
        "관심 지역 (선택 안 하면 전체)",
        options=list(SEOUL_GU_CODES.keys()),
        default=[],
    )

    collect_btn = st.button("📊 실거래가 수집 및 분석", type="primary", use_container_width=True)


# --- 데이터 수집 ---
if collect_btn:
    if not selected_types:
        st.error("매물 타입을 최소 1개 선택해주세요.")
    else:
        type_label = ", ".join(selected_types)
        with st.spinner(f"{type_label} 실거래가 데이터 수집 중... (2~5분 소요)"):
            trade_df = collect_seoul_trades(months=months, property_types=selected_types)
            rent_df = collect_seoul_rents(months=months, property_types=selected_types)

        st.session_state["trade_df"] = trade_df
        st.session_state["rent_df"] = rent_df
        st.session_state["selected_types"] = selected_types

        if trade_df.empty:
            st.warning("수집된 데이터가 없습니다. API 신청 여부를 확인해주세요.")
        else:
            st.success(f"수집 완료! 매매 {len(trade_df):,}건, 전월세 {len(rent_df):,}건")


# --- 탭 구성 ---
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📈 시장 현황", "🗺️ 구별 분석", "💰 전세가율",
    "🎯 투자 후보", "🏘️ 현재 매물", "🏗️ 모아타운 예측", "🤖 AI 리포트"
])

# --- 탭5: 현재 매물 ---
with tab5:
    st.subheader("🏘️ 현재 매물 / 단지 정보")

    # 데이터 소스 선택
    data_source = st.radio(
        "데이터 소스",
        ["직방 (현재 매물 + 호가) ✅추천", "네이버 부동산 (현재 매물 + 호가)", "KB부동산 (단지 정보 + KB시세)"],
        horizontal=True,
        key="data_source",
    )

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        listing_gu = st.selectbox("구 선택", options=list(SEOUL_GU_CORTAR.keys()), index=14, key="listing_gu")
    with col_b:
        listing_dong = st.text_input("동 입력 (선택사항)", value="", placeholder="예: 반포", key="listing_dong",
                                      help="비워두면 구 전체 검색")
    with col_c:
        listing_type = st.selectbox("매물 타입", options=list(REAL_ESTATE_TYPES.keys()), index=1, key="listing_type")

    # --- 직방 ---
    if "직방" in data_source:
        zb_trades = st.multiselect(
            "거래 타입",
            options=["매매", "전세", "월세"],
            default=["매매", "전세"],
            key="zb_trades",
        )

        zb_btn = st.button("🔎 직방 매물 검색", type="primary", key="zb_btn")

        if zb_btn:
            with st.spinner(f"{listing_gu} {listing_type} 매물 검색 중..."):
                zb_df = zigbang_crawl(listing_gu, property_type=listing_type, sales_types=zb_trades)

                # 동 필터링
                if listing_dong and not zb_df.empty:
                    zb_df = zb_df[zb_df["동"].str.contains(listing_dong, na=False)].copy()

            st.session_state["zb_df"] = zb_df

        if "zb_df" in st.session_state and not st.session_state["zb_df"].empty:
            zb_df = st.session_state["zb_df"]
            st.success(f"총 {len(zb_df):,}건 매물 발견")

            # 거래타입별 요약
            if "거래타입" in zb_df.columns:
                trade_summary = zb_df.groupby("거래타입").agg(
                    매물수=("보증금(만원)", "count"),
                    평균보증금=("보증금(만원)", "mean"),
                    최저보증금=("보증금(만원)", "min"),
                    최고보증금=("보증금(만원)", "max"),
                ).round(0)
                st.dataframe(trade_summary.style.format("{:,.0f}"), use_container_width=True)

            # 보증금 분포 차트
            if len(zb_df) > 1:
                chart_df = zb_df[zb_df["보증금(만원)"] > 0]
                if not chart_df.empty:
                    fig = px.histogram(
                        chart_df, x="보증금(만원)", color="거래타입", nbins=20,
                        title="보증금 분포", labels={"보증금(만원)": "보증금 (만원)"},
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # 매물 상세 테이블
            st.subheader("매물 상세")
            trade_types_in_data = zb_df["거래타입"].unique().tolist()
            if len(trade_types_in_data) > 1:
                filter_trade = st.radio("거래타입 필터", ["전체"] + trade_types_in_data, horizontal=True, key="zb_filter")
                if filter_trade != "전체":
                    zb_df = zb_df[zb_df["거래타입"] == filter_trade]

            display_cols = ["매물명", "동", "거래타입", "보증금(만원)", "월세(만원)", "전용면적", "평", "층", "총층", "방향", "방타입", "승인일(준공)", "입주가능일", "매물설명"]
            available_cols = [c for c in display_cols if c in zb_df.columns]
            st.dataframe(
                zb_df[available_cols].sort_values("보증금(만원)", ascending=True),
                use_container_width=True, height=500,
            )

            # 호가 vs 실거래가 비교
            if "trade_df" in st.session_state and not st.session_state["trade_df"].empty:
                st.divider()
                st.subheader("📊 호가 vs 실거래가 비교")
                trade_df_compare = st.session_state["trade_df"]
                gu_trades = trade_df_compare[trade_df_compare["구"] == listing_gu] if "구" in trade_df_compare.columns else pd.DataFrame()
                sale_listings = zb_df[zb_df["거래타입"] == "매매"] if "거래타입" in zb_df.columns else pd.DataFrame()

                if not gu_trades.empty and not sale_listings.empty:
                    avg_real = int(gu_trades["거래금액"].mean())
                    avg_listing = int(sale_listings["보증금(만원)"].mean())
                    diff = avg_listing - avg_real
                    diff_pct = (diff / avg_real * 100) if avg_real > 0 else 0

                    col1, col2, col3 = st.columns(3)
                    col1.metric("실거래 평균", f"{avg_real:,}만원")
                    col2.metric("호가 평균", f"{avg_listing:,}만원")
                    col3.metric("호가 프리미엄", f"{diff:+,}만원 ({diff_pct:+.1f}%)", delta_color="inverse")

        elif "zb_df" in st.session_state:
            st.warning("해당 지역에 매물이 없습니다.")

    # --- 네이버 부동산 ---
    elif "네이버" in data_source:
        naver_trades = st.multiselect(
            "거래 타입",
            options=["매매", "전세", "월세"],
            default=["매매", "전세"],
            key="naver_trades",
        )

        crawl_btn = st.button("🔎 네이버 매물 검색", type="primary", key="crawl_btn")

        if crawl_btn:
            with st.spinner(f"{listing_gu} {listing_dong} {listing_type} 매물 검색 중... (rate limit 시 시간 소요)"):
                if listing_dong:
                    listing_df = crawl_dong_listings(
                        listing_gu, listing_dong,
                        property_type=listing_type,
                        trade_types=naver_trades,
                    )
                else:
                    listing_df = pd.DataFrame()
                    for trade_name in naver_trades:
                        df = crawl_gu_listings(listing_gu, property_type=listing_type, trade_type_name=trade_name)
                        listing_df = pd.concat([listing_df, df], ignore_index=True)

            st.session_state["listing_df"] = listing_df

        if "listing_df" in st.session_state and not st.session_state["listing_df"].empty:
            listing_df = st.session_state["listing_df"]
            st.success(f"총 {len(listing_df):,}건 매물 발견")

            if "거래타입" in listing_df.columns:
                trade_summary = listing_df.groupby("거래타입").agg(
                    매물수=("호가(만원)", "count"),
                    평균호가=("호가(만원)", "mean"),
                    최저호가=("호가(만원)", "min"),
                    최고호가=("호가(만원)", "max"),
                ).round(0)
                st.dataframe(trade_summary.style.format("{:,.0f}"), use_container_width=True)

            if len(listing_df) > 1:
                fig = px.histogram(
                    listing_df[listing_df["호가(만원)"] > 0],
                    x="호가(만원)", color="거래타입", nbins=20,
                    title="호가 분포", labels={"호가(만원)": "호가 (만원)"},
                )
                st.plotly_chart(fig, use_container_width=True)

            st.subheader("매물 상세")
            trade_types_in_data = listing_df["거래타입"].unique().tolist()
            if len(trade_types_in_data) > 1:
                filter_trade = st.radio("거래타입 필터", ["전체"] + trade_types_in_data, horizontal=True, key="listing_filter")
                if filter_trade != "전체":
                    listing_df = listing_df[listing_df["거래타입"] == filter_trade]

            display_cols = ["매물명", "동", "거래타입", "호가", "전용면적", "평", "층정보", "확인일자", "매물설명"]
            available_cols = [c for c in display_cols if c in listing_df.columns]
            st.dataframe(
                listing_df[available_cols].sort_values("호가(만원)" if "호가(만원)" in listing_df.columns else available_cols[0], ascending=True),
                use_container_width=True, height=500,
            )

            # 호가 vs 실거래가 비교
            if "trade_df" in st.session_state and not st.session_state["trade_df"].empty:
                st.divider()
                st.subheader("📊 호가 vs 실거래가 비교")
                trade_df_compare = st.session_state["trade_df"]
                gu_trades = trade_df_compare[trade_df_compare["구"] == listing_gu] if "구" in trade_df_compare.columns else pd.DataFrame()
                sale_listings = listing_df[listing_df["거래타입"] == "매매"] if "거래타입" in listing_df.columns else pd.DataFrame()

                if not gu_trades.empty and not sale_listings.empty:
                    avg_real = int(gu_trades["거래금액"].mean())
                    avg_listing = int(sale_listings["호가(만원)"].mean())
                    diff = avg_listing - avg_real
                    diff_pct = (diff / avg_real * 100) if avg_real > 0 else 0

                    col1, col2, col3 = st.columns(3)
                    col1.metric("실거래 평균", f"{avg_real:,}만원")
                    col2.metric("호가 평균", f"{avg_listing:,}만원")
                    col3.metric("호가 프리미엄", f"{diff:+,}만원 ({diff_pct:+.1f}%)", delta_color="inverse")

        elif "listing_df" in st.session_state:
            st.warning("매물이 없거나 네이버 rate limit에 걸렸습니다. 잠시 후 다시 시도하거나 KB부동산을 이용해주세요.")

    # --- KB부동산 ---
    elif "KB" in data_source:
        st.caption("KB부동산에서 단지 정보를 조회합니다 (rate limit 없음)")

        kb_btn = st.button("🔎 KB 단지 정보 검색", type="primary", key="kb_btn")

        if kb_btn:
            with st.spinner(f"{listing_gu} {listing_type} 단지 정보 수집 중..."):
                kb_df = crawl_area_kb(
                    listing_gu,
                    dong_name=listing_dong if listing_dong else None,
                    property_type=listing_type,
                )
            st.session_state["kb_df"] = kb_df

        if "kb_df" in st.session_state and not st.session_state["kb_df"].empty:
            kb_df = st.session_state["kb_df"]
            st.success(f"총 {len(kb_df):,}개 단지 조회 완료")

            # 요약 통계
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("총 단지 수", f"{len(kb_df):,}개")
            col2.metric("총 세대 수", f"{int(kb_df['총세대수'].sum()):,}세대" if "총세대수" in kb_df.columns else "-")
            if "준공년월" in kb_df.columns:
                old = kb_df[kb_df["준공년월"].str[:4].apply(lambda x: x.isdigit() and int(x) < 2000 if x else False)]
                col3.metric("20년 이상 노후 단지", f"{len(old):,}개")
            if "재건축여부" in kb_df.columns:
                recon = kb_df[kb_df["재건축여부"] == "1"]
                col4.metric("재건축 대상", f"{len(recon):,}개")

            # 단지 목록
            st.subheader("단지 목록")
            display_cols = ["단지명", "매물타입", "준공년월", "총세대수", "최소전용면적", "최대전용면적", "재건축여부"]
            available = [c for c in display_cols if c in kb_df.columns]
            st.dataframe(kb_df[available].sort_values("총세대수", ascending=False) if "총세대수" in kb_df.columns else kb_df[available],
                        use_container_width=True, height=500)

            # 준공년도별 분포
            if "준공년월" in kb_df.columns:
                kb_year = kb_df.copy()
                kb_year["준공년도"] = kb_year["준공년월"].str[:4]
                kb_year = kb_year[kb_year["준공년도"].str.isdigit()]
                kb_year["준공년도"] = kb_year["준공년도"].astype(int)
                if not kb_year.empty:
                    fig = px.histogram(kb_year, x="준공년도", nbins=20, title="준공년도 분포")
                    st.plotly_chart(fig, use_container_width=True)

        elif "kb_df" in st.session_state:
            st.warning("조회된 단지가 없습니다.")


# --- 탭6: 모아타운 예측 ---
with tab6:
    st.subheader("🏗️ 모아타운 선정 가능성 예측")
    st.caption("건축물대장 데이터 기반 노후도 분석 → 미선정 지역 중 유망 지역 스코어링")

    data_path = Path("data/seoul_obsolescence.csv")

    # --- 개별 동 진단 ---
    st.divider()
    st.subheader("🔎 개별 동 진단")
    st.caption("동 이름을 입력하면 모아타운 선정 가능성을 상세 진단합니다 (예: 번동, 반포1동, 면목동)")

    col_input, col_btn = st.columns([3, 1])
    with col_input:
        dong_input = st.text_input("동 이름 입력", placeholder="예: 번동, 면목동, 수유동, 반포1동", key="dong_input")
    with col_btn:
        st.write("")
        st.write("")
        diag_btn = st.button("진단 시작", type="primary", key="diag_btn")

    if diag_btn and dong_input:
        with st.spinner(f"'{dong_input}' 진단 중... (건축물대장 조회)"):
            diag = diagnose_dong(dong_input)

        if "error" in diag and "score" not in diag:
            st.error(diag["error"])
        elif "error" in diag:
            st.warning(diag["error"])
        else:
            st.session_state["diag_result"] = diag
            # 진단 결과 자동 저장
            if diag.get("blocks"):
                save_dir = Path("data/dong_diag")
                save_dir.mkdir(parents=True, exist_ok=True)
                dong_df = pd.DataFrame(diag["blocks"])
                dong_df["구"] = diag.get("land_price", {}).get("source", "")  # placeholder
                # 지번에서 구 이름 추출
                sample = dong_df["검색주소"].iloc[0] if not dong_df.empty else ""
                for gu in SEOUL_GU_CODES.keys():
                    if gu in sample:
                        dong_df["구"] = gu
                        break
                dong_df["동"] = diag["dong"]
                save_path = save_dir / f"{diag['dong']}.csv"
                dong_df.to_csv(save_path, index=False, encoding="utf-8-sig")

    # 이전 진단 기록 불러오기
    diag_dir = Path("data/dong_diag")
    if diag_dir.exists():
        saved_files = sorted(diag_dir.glob("*.csv"))
        if saved_files:
            with st.expander(f"📁 이전 진단 기록 ({len(saved_files)}개 동)"):
                selected = st.selectbox("불러올 동 선택", [f.stem for f in saved_files], key="load_diag")
                if st.button("불러오기", key="load_diag_btn"):
                    loaded = pd.read_csv(diag_dir / f"{selected}.csv")
                    st.session_state["diag_result"] = {
                        "dong": selected,
                        "is_designated": any(d == selected for _, d, _, _ in DESIGNATED_MOATOWNS),
                        "designated_list": [
                            {"구": gu, "동": dong, "대표지번": jibun, "선정일": date}
                            for gu, dong, jibun, date in DESIGNATED_MOATOWNS if dong == selected
                        ],
                        "blocks": loaded.to_dict("records"),
                        "score": {"종합_점수": 0, "등급": "-"},
                    }
                    st.rerun()

    if "diag_result" in st.session_state:
        diag = st.session_state["diag_result"]
        if "score" in diag and diag.get("blocks"):
            blocks_df = pd.DataFrame(diag["blocks"])
            meets = blocks_df[blocks_df["요건충족"] == True]
            designated_tag = " (기선정 지역)" if diag.get("is_designated") else ""

            st.markdown(f"### 📍 {diag['dong']} 블록별 진단 결과{designated_tag}")

            # 기선정 지역 표시
            if diag.get("designated_list"):
                dl = diag["designated_list"]
                st.success(f"이 동에 이미 선정된 모아타운 **{len(dl)}개소**가 있습니다")
                with st.expander(f"기선정 모아타운 {len(dl)}개소 보기"):
                    des_df = pd.DataFrame(dl)
                    des_df["검색주소"] = des_df.apply(lambda r: f"{r['구']} {r['동']} {r['대표지번']}", axis=1)
                    st.dataframe(des_df[["구", "동", "대표지번", "선정일", "검색주소"]], use_container_width=True)

            st.caption("블록 단위로 분석합니다. 모아타운은 동 전체가 아닌 블록 단위로 선정됩니다.")

            col1, col2, col3 = st.columns(3)
            col1.metric("분석 블록 수", f"{len(blocks_df)}개")
            col2.metric("요건 충족 블록", f"{len(meets)}개", help="노후도 50% 이상")
            col3.metric("최고 점수 블록", f"{blocks_df['블록점수'].max():.1f}점" if not blocks_df.empty else "-")

            # 블록별 점수 차트
            fig_blocks = px.bar(
                blocks_df, x="구간", y="블록점수",
                color="요건충족",
                color_discrete_map={True: "#2ecc71", False: "#e74c3c"},
                title="블록별 모아타운 예측 점수 (점수순)",
                hover_data=["노후도", "주거건물", "교회", "상업시설", "평균나이", "문제점"],
            )
            st.plotly_chart(fig_blocks, use_container_width=True)

            # 전체 블록 테이블 (점수순)
            display_cols = ["구간", "검색주소", "블록점수", "등급", "노후도", "충족상태", "주거건물", "평균나이", "교회", "상업시설", "문제점"]
            available = [c for c in display_cols if c in blocks_df.columns]
            st.dataframe(
                blocks_df[available]
                    .style.format({"노후도": "{:.1f}%", "평균나이": "{:.0f}년", "블록점수": "{:.1f}"})
                    .background_gradient(subset=["블록점수"], cmap="RdYlGn"),
                use_container_width=True, height=400,
            )

            # 블록별 상세 + 지도 (구간 클릭하면 지도 팝업)
            st.divider()
            st.markdown("### 블록 상세 (클릭하면 지도)")
            import folium
            from streamlit_folium import st_folium
            import requests as req

            kakao_key = os.environ.get("KAKAO_REST_API_KEY", "")
            vworld_key = os.environ.get("VWORLD_API_KEY", "")

            for idx, (_, blk) in enumerate(blocks_df.iterrows()):
                score = blk.get("블록점수", 0)
                구간 = str(blk.get("구간", ""))
                충족 = blk.get("충족상태", "")
                emoji = "🟢" if score >= 70 else ("🟡" if score >= 55 else ("🟠" if score >= 40 else "🔴"))

                with st.expander(f"{emoji} **{구간}** | {score:.1f}점 | 노후도 {blk.get('노후도',0):.1f}% | {충족}"):
                    c1, c2, c3, c4, c5 = st.columns(5)
                    c1.metric("점수", f"{score:.1f}")
                    c2.metric("노후도", f"{blk.get('노후도',0):.1f}%")
                    c3.metric("주거건물", f"{blk.get('주거건물',0)}건")
                    c4.metric("교회", f"{blk.get('교회',0)}개")
                    c5.metric("상업시설", f"{blk.get('상업시설',0)}개")

                    문제 = blk.get("문제점", "-")
                    if 문제 and 문제 != "-":
                        st.warning(f"문제점: {문제}")
                    else:
                        st.success("특이 문제 없음")

                    # 지도 렌더링
                    addr = str(blk.get("검색주소", "")).replace("번지", "").strip()
                    lat, lon = None, None
                    if kakao_key and addr:
                        try:
                            kr = req.get("https://dapi.kakao.com/v2/local/search/address.json",
                                params={"query": addr}, headers={"Authorization": f"KakaoAK {kakao_key}"}, timeout=5)
                            docs = kr.json().get("documents", [])
                            if docs:
                                lat, lon = float(docs[0]["y"]), float(docs[0]["x"])
                        except Exception:
                            pass

                    if lat:
                        bm = folium.Map(location=[lat, lon], zoom_start=17, tiles="cartodbpositron")

                        if score >= 70: fill_color = "#27ae60"
                        elif score >= 55: fill_color = "#f39c12"
                        elif score >= 40: fill_color = "#e67e22"
                        else: fill_color = "#e74c3c"

                        # V-World 필지 폴리곤
                        polygon_drawn = False
                        if vworld_key:
                            try:
                                bbox = f"BOX({lon-0.003},{lat-0.003},{lon+0.003},{lat+0.003})"
                                bonbuns = []
                                if "~" in 구간:
                                    parts = 구간.replace("번지", "").split("~")
                                    try: bonbuns = list(range(int(parts[0]), int(parts[1]) + 1))
                                    except: pass
                                elif "번지" in 구간:
                                    try: bonbuns = [int(구간.replace("번지", ""))]
                                    except: pass

                                if bonbuns:
                                    vr = req.get('https://api.vworld.kr/req/data', params={
                                        'service': 'data', 'request': 'GetFeature',
                                        'data': 'LP_PA_CBND_BUBUN', 'key': vworld_key,
                                        'domain': 'localhost', 'geomFilter': bbox,
                                        'crs': 'EPSG:4326', 'format': 'json', 'size': '1000',
                                    }, timeout=10)
                                    vd = vr.json()
                                    if vd.get('response', {}).get('status') == 'OK':
                                        feats = vd['response']['result']['featureCollection']['features']
                                        for feat in feats:
                                            bn = int(feat['properties'].get('bonbun', 0) or 0)
                                            if bn in bonbuns:
                                                for poly in feat['geometry']['coordinates']:
                                                    for ring in poly:
                                                        latlng = [[c[1], c[0]] for c in ring]
                                                        folium.Polygon(
                                                            locations=latlng, color=fill_color, fill=True,
                                                            fill_color=fill_color, fill_opacity=0.5, weight=2,
                                                        ).add_to(bm)
                                                        polygon_drawn = True
                            except Exception:
                                pass

                        if not polygon_drawn:
                            folium.CircleMarker(
                                location=[lat, lon], radius=30,
                                color=fill_color, fill=True, fill_color=fill_color, fill_opacity=0.4,
                            ).add_to(bm)

                        st_folium(bm, width=None, height=350, key=f"block_map_{idx}")

        elif "score" in diag:
            st.warning(f"{diag['dong']}: 블록 분석 데이터가 부족합니다.")

    st.divider()
    st.subheader("📊 서울 전역 스캔")
    st.caption("1단계: 동 스캔 (3~5분) → 2단계: 블록 상세 분석 (30분~1시간) → 결과 자동 저장")

    blocks_path = Path("data/seoul_blocks.csv")

    col_scan, col_block, col_load = st.columns(3)
    with col_scan:
        if st.button("🔍 1단계: 동 스캔", type="primary", key="moatown_scan"):
            progress_bar = st.progress(0)
            status_text = st.empty()

            def update_progress(current, total, dong_name):
                progress_bar.progress(current / total)
                status_text.text(f"[{current}/{total}] {dong_name} 분석 중...")

            with st.spinner("서울 전역 동별 노후도 수집 중..."):
                obs_df = collect_all_seoul(progress_callback=update_progress)

            st.session_state["obs_df"] = obs_df
            progress_bar.progress(1.0)
            status_text.text("완료!")
            st.success(f"{len(obs_df)}개 동 스캔 완료! → 2단계 블록 분석을 눌러주세요")

    with col_block:
        if st.button("🧱 2단계: 블록 분석", key="moatown_block"):
            if "obs_df" not in st.session_state:
                if data_path.exists():
                    st.session_state["obs_df"] = pd.read_csv(data_path)
                else:
                    st.error("먼저 1단계 동 스캔을 실행해주세요")
                    st.stop()

            obs_df = st.session_state["obs_df"]
            progress_bar = st.progress(0)
            status_text = st.empty()
            all_blocks = []
            total = len(obs_df)

            for idx, (_, row) in enumerate(obs_df.iterrows()):
                progress_bar.progress((idx + 1) / total)
                status_text.text(f"[{idx+1}/{total}] {row['구']} {row['동']} 블록 분석 중...")
                try:
                    result = diagnose_dong(row["동"], use_coords=False)
                    if result.get("blocks"):
                        for b in result["blocks"]:
                            b["구"] = row["구"]
                            b["동"] = row["동"]
                        all_blocks.extend(result["blocks"])
                except Exception:
                    pass

            blocks_df = pd.DataFrame(all_blocks) if all_blocks else pd.DataFrame()
            if not blocks_df.empty:
                Path("data").mkdir(exist_ok=True)
                blocks_df.to_csv(blocks_path, index=False, encoding="utf-8-sig")

            st.session_state["blocks_scan_df"] = blocks_df
            progress_bar.progress(1.0)
            status_text.text("완료!")
            st.success(f"{total}개 동 → {len(blocks_df)}개 블록 발견! (자동 저장됨)")

    with col_load:
        if data_path.exists() or blocks_path.exists():
            if st.button("📂 저장된 데이터 불러오기", key="moatown_load"):
                if blocks_path.exists():
                    blocks_df = pd.read_csv(blocks_path)
                    st.session_state["blocks_scan_df"] = blocks_df
                    st.success(f"블록 데이터 로드: {len(blocks_df)}개 블록")
                elif data_path.exists():
                    obs_df = pd.read_csv(data_path)
                    st.session_state["obs_df"] = obs_df
                    st.success(f"동 데이터 로드: {len(obs_df)}개 동 → 2단계 블록 분석을 눌러주세요")

    # === 블록 단위 결과 표시 ===
    if "blocks_scan_df" in st.session_state and not st.session_state["blocks_scan_df"].empty:
        blocks_df = st.session_state["blocks_scan_df"]
        scorer = MoatownScorer()

        # 기선정 동 마킹
        designated_dongs = set()
        for gu, dong, jibun, date in DESIGNATED_MOATOWNS:
            designated_dongs.add(dong)
        blocks_df["기선정동"] = blocks_df["동"].apply(lambda d: "✅" if d in designated_dongs else "")

        meets = blocks_df[blocks_df["요건충족"] == True]
        not_des_blocks = meets[meets["기선정동"] == ""]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("총 블록 수", f"{len(blocks_df)}개")
        col2.metric("요건 충족 블록", f"{len(meets)}개")
        col3.metric("기선정 동 블록", f"{len(meets[meets['기선정동'] != ''])}개")
        col4.metric("미선정 유망 블록", f"{len(not_des_blocks)}개")

        sub1, sub2, sub3 = st.tabs(["🎯 유망 블록 랭킹", "🗺️ 지도", "📊 전체 블록"])

        with sub1:
            st.subheader("미선정 지역 유망 블록 TOP 50")
            if not not_des_blocks.empty:
                display = not_des_blocks.nlargest(50, "블록점수")
                display_cols = ["구", "동", "구간", "검색주소", "블록점수", "등급", "노후도", "충족상태", "주거건물", "평균나이", "교회", "상업시설", "문제점"]
                available = [c for c in display_cols if c in display.columns]
                st.dataframe(
                    display[available]
                        .style.format({"노후도": "{:.1f}%", "평균나이": "{:.0f}년", "블록점수": "{:.1f}"})
                        .background_gradient(subset=["블록점수"], cmap="RdYlGn"),
                    use_container_width=True, height=600,
                )
            else:
                st.info("미선정 유망 블록이 없습니다.")

        with sub2:
            st.subheader("🗺️ 블록 지도")
            import folium
            from streamlit_folium import st_folium

            map_filter = st.radio("표시할 블록", ["요건 충족 블록만", "전체 블록"], horizontal=True, key="map_filter")
            map_df = blocks_df.copy()
            if map_filter == "요건 충족 블록만":
                map_df = map_df[map_df["요건충족"] == True]

            if map_df.empty:
                st.info("표시할 블록이 없습니다.")
            else:
                # 좌표가 있는 블록은 마커, 없으면 검색주소로 좌표 조회
                m = folium.Map(location=[37.5665, 126.9780], zoom_start=11, tiles="cartodbpositron")
                vworld_key = os.environ.get("VWORLD_API_KEY", "")

                for _, row in map_df.iterrows():
                    score = row.get("블록점수", 0)
                    lat = row.get("lat") if "lat" in row and pd.notna(row.get("lat")) else None
                    lon = row.get("lon") if "lon" in row and pd.notna(row.get("lon")) else None

                    # 점수별 색상 (폴리곤용)
                    if score >= 70:
                        fill_color = "#27ae60"
                    elif score >= 55:
                        fill_color = "#f39c12"
                    elif score >= 40:
                        fill_color = "#e67e22"
                    else:
                        fill_color = "#e74c3c"

                    is_des = "기선정동" in row and row.get("기선정동") == "✅"

                    popup_text = (
                        f"<b>{row.get('구', '')} {row.get('동', '')} {row.get('구간', '')}</b><br>"
                        f"점수: {score:.1f}점<br>"
                        f"노후도: {row.get('노후도', 0):.1f}%<br>"
                        f"충족: {row.get('충족상태', '')}<br>"
                        f"교회: {row.get('교회', 0)}개 | 상업: {row.get('상업시설', 0)}개<br>"
                        f"{'⭐ 기선정 지역' if is_des else ''}"
                    )

                    # V-World 폴리곤 시도 (좌표가 있는 경우 BBOX로)
                    polygon_drawn = False
                    if vworld_key and lat and lon:
                        try:
                            import requests as req
                            bbox = f"BOX({lon-0.002},{lat-0.002},{lon+0.002},{lat+0.002})"
                            # 구간에서 본번 범위 추출
                            구간 = str(row.get("구간", ""))
                            bonbuns = []
                            if "~" in 구간:
                                parts = 구간.replace("번지", "").split("~")
                                try:
                                    start, end = int(parts[0]), int(parts[1])
                                    bonbuns = list(range(start, end + 1))
                                except:
                                    pass
                            elif "번지" in 구간:
                                try:
                                    bonbuns = [int(구간.replace("번지", ""))]
                                except:
                                    pass

                            if bonbuns:
                                vr = req.get('https://api.vworld.kr/req/data', params={
                                    'service': 'data', 'request': 'GetFeature',
                                    'data': 'LP_PA_CBND_BUBUN', 'key': vworld_key,
                                    'domain': 'localhost', 'geomFilter': bbox,
                                    'crs': 'EPSG:4326', 'format': 'json', 'size': '1000',
                                }, timeout=10)
                                vd = vr.json()
                                if vd.get('response', {}).get('status') == 'OK':
                                    feats = vd['response']['result']['featureCollection']['features']
                                    for feat in feats:
                                        bn = int(feat['properties'].get('bonbun', 0) or 0)
                                        if bn in bonbuns:
                                            coords = feat['geometry']['coordinates']
                                            # MultiPolygon → 좌표 변환 [lon,lat] → [lat,lon]
                                            for poly in coords:
                                                for ring in poly:
                                                    latlng = [[c[1], c[0]] for c in ring]
                                                    folium.Polygon(
                                                        locations=latlng,
                                                        color=fill_color, fill=True,
                                                        fill_color=fill_color, fill_opacity=0.5,
                                                        weight=2, opacity=0.8,
                                                        popup=folium.Popup(popup_text, max_width=250),
                                                        tooltip=f"{row.get('동','')} {구간} ({score:.0f}점)",
                                                    ).add_to(m)
                                                    polygon_drawn = True
                        except Exception:
                            pass

                    # 폴리곤 못 그리면 마커로 폴백
                    if not polygon_drawn and lat and lon:
                        icon_color = "green" if score >= 55 else ("orange" if score >= 40 else "red")
                        folium.Marker(
                            location=[lat, lon],
                            popup=folium.Popup(popup_text, max_width=250),
                            tooltip=f"{row.get('동','')} {row.get('구간','')} ({score:.0f}점)",
                            icon=folium.Icon(color=icon_color, icon="star" if is_des else "home", prefix="fa"),
                        ).add_to(m)

                st_folium(m, width=None, height=600)
                st.caption("🟢 70점+ | 🟡 55점+ | 🟠 40점+ | 🔴 40점 미만 | 면: 필지 경계 | 핀: 좌표만")

        with sub3:
            st.subheader("전체 블록 (점수순)")
            fig = px.bar(
                blocks_df.nlargest(50, "블록점수"),
                x=blocks_df.nlargest(50, "블록점수").apply(lambda r: f"{r['동']} {r['구간']}", axis=1),
                y="블록점수", color="요건충족",
                color_discrete_map={True: "#2ecc71", False: "#e74c3c"},
                title="블록별 점수 TOP 50",
            )
            st.plotly_chart(fig, use_container_width=True)

            display_cols = ["구", "동", "구간", "검색주소", "블록점수", "등급", "노후도", "충족상태", "교회", "상업시설", "기선정동", "문제점"]
            available = [c for c in display_cols if c in blocks_df.columns]
            st.dataframe(
                blocks_df.sort_values("블록점수", ascending=False)[available]
                    .style.format({"노후도": "{:.1f}%", "블록점수": "{:.1f}"}),
                use_container_width=True, height=500,
            )

    # === 동 단위 결과 (블록 스캔 전 or 저장 데이터) ===
    elif "obs_df" in st.session_state and not st.session_state["obs_df"].empty:
        obs_df = st.session_state["obs_df"]
        scorer = MoatownScorer()

        obs_df["기선정"] = obs_df.apply(
            lambda r: "✅" if any(d == r["동"] for _, d, _, _ in DESIGNATED_MOATOWNS) else "", axis=1
        )

        meets_req = obs_df[obs_df["노후도_30년"] >= 50].copy()

        st.info("동 단위 데이터입니다. '서울 전역 스캔'을 실행하면 블록 단위 상세 분석 결과를 볼 수 있습니다.")

        fig = px.bar(
            obs_df.nlargest(40, "노후도_30년"),
            x="동", y="노후도_30년", color="구",
            title="노후도 TOP 40 (30년+ 건물 비율)",
        )
        fig.add_hline(y=50, line_dash="dash", line_color="red", annotation_text="필수요건 50%")
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            obs_df.sort_values("노후도_30년", ascending=False)
                [["구", "동", "노후도_30년", "주거용건물수", "평균건물나이", "기선정"]]
                .style.format({"노후도_30년": "{:.1f}%", "평균건물나이": "{:.0f}년"}),
            use_container_width=True, height=500,
        )


# --- 나머지 탭 (실거래가 데이터 기반) ---
if "trade_df" in st.session_state:
    trade_df = st.session_state["trade_df"]
    rent_df = st.session_state["rent_df"]
    active_types = st.session_state.get("selected_types", ["아파트"])

    # 관심 지역 필터링
    if selected_gus:
        trade_df = trade_df[trade_df["구"].isin(selected_gus)]
        rent_df = rent_df[rent_df["구"].isin(selected_gus)]

    # 매물타입 필터
    if len(active_types) > 1 and not trade_df.empty:
        type_filter = st.radio(
            "매물타입 필터",
            options=["전체"] + active_types,
            horizontal=True,
        )
        if type_filter != "전체":
            trade_df = trade_df[trade_df["매물타입"] == type_filter]
            rent_df = rent_df[rent_df["매물타입"] == type_filter] if not rent_df.empty else rent_df

    # --- 탭1: 시장 현황 ---
    with tab1:
        summary = get_market_summary(trade_df, rent_df)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("총 거래건수", f"{summary['총거래건수']:,}건")
        col2.metric("평균 매매가", f"{summary['평균매매가']:,}만원")
        col3.metric("중위 매매가", f"{summary['중위매매가']:,}만원")
        col4.metric("평균 평당가격", f"{summary['평균평당가격']:,}만원")

        if len(active_types) > 1 and "매물타입" in trade_df.columns:
            st.subheader("매물타입별 거래 현황")
            type_counts = trade_df.groupby("매물타입").agg(
                거래건수=("거래금액", "count"),
                평균매매가=("거래금액", "mean"),
                중위매매가=("거래금액", "median"),
            ).round(0)
            st.dataframe(type_counts.style.format("{:,.0f}"), use_container_width=True)

        st.subheader("월별 매매가 추세")
        trend = analyze_price_trend(trade_df)
        if not trend.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trend.index, y=trend["평균매매가"],
                mode="lines+markers", name="평균 매매가",
            ))
            fig.add_trace(go.Scatter(
                x=trend.index, y=trend["중위매매가"],
                mode="lines+markers", name="중위 매매가",
            ))
            fig.update_layout(yaxis_title="가격 (만원)", xaxis_title="거래년월")
            st.plotly_chart(fig, use_container_width=True)

            fig2 = px.bar(trend.reset_index(), x="거래년월", y="거래건수", title="월별 거래량")
            st.plotly_chart(fig2, use_container_width=True)

    # --- 탭2: 구별 분석 ---
    with tab2:
        st.subheader("구별 매매 시세 비교")
        gu_analysis = analyze_by_gu(trade_df)
        if not gu_analysis.empty:
            fig = px.bar(
                gu_analysis.reset_index(),
                x="구", y="평균매매가",
                color="평균평당가격",
                title="구별 평균 매매가",
                color_continuous_scale="RdYlGn_r",
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(gu_analysis.style.format("{:,.0f}"), use_container_width=True)

    # --- 탭3: 전세가율 ---
    with tab3:
        st.subheader("구별 전세가율 분석")
        st.caption("전세가율이 높을수록 → 갭투자에 유리 (적은 자본으로 매수 가능)")

        jeonse_ratio = calculate_jeonse_ratio(trade_df, rent_df)
        if not jeonse_ratio.empty:
            fig = px.bar(
                jeonse_ratio,
                x="구", y="전세가율",
                color="전세가율",
                title="구별 전세가율 (%)",
                color_continuous_scale="RdYlGn",
            )
            fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="전세가율 70%")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(
                jeonse_ratio.style.format({
                    "매매중위가": "{:,.0f}",
                    "전세중위가": "{:,.0f}",
                    "전세가율": "{:.1f}%",
                    "갭(매매-전세)": "{:,.0f}",
                }),
                use_container_width=True,
            )

    # --- 탭4: 투자 후보 ---
    with tab4:
        st.subheader(f"투자 후보 매물 (예산: {budget:,}만원 / {budget/10000:.1f}억)")

        candidates = find_investment_candidates(trade_df, rent_df, budget, strategy)
        if not candidates.empty:
            st.info(f"총 {len(candidates):,}개 매물 발견")

            if strategy == "gap":
                st.dataframe(
                    candidates.head(50).style.format({
                        "매매중위가": "{:,.0f}",
                        "전세중위가": "{:,.0f}",
                        "갭": "{:,.0f}",
                        "전세가율": "{:.1f}%",
                        "평균평당가격": "{:,.0f}",
                        "평균면적": "{:.1f}",
                    }),
                    use_container_width=True,
                )
            else:
                st.dataframe(
                    candidates.head(50)[["구", "매물명", "법정동", "전용면적", "평", "거래금액", "평당가격", "층"]].style.format({
                        "거래금액": "{:,.0f}",
                        "평당가격": "{:,.0f}",
                        "전용면적": "{:.1f}",
                        "평": "{:.1f}",
                    }),
                    use_container_width=True,
                )
        else:
            st.warning("예산 범위 내 매물이 없습니다. 예산을 늘리거나 전략을 변경해보세요.")

    # --- 탭6: AI 리포트 ---
    with tab7:
        st.subheader("🤖 AI 투자 분석 리포트")

        gu_analysis = analyze_by_gu(trade_df)
        jeonse_ratio = calculate_jeonse_ratio(trade_df, rent_df)
        candidates = find_investment_candidates(trade_df, rent_df, budget, strategy)
        summary = get_market_summary(trade_df, rent_df)

        if st.button("📝 AI 리포트 생성", type="primary"):
            with st.spinner("AI가 투자 분석 리포트를 작성 중..."):
                report = generate_investment_report(
                    market_summary=summary,
                    gu_analysis=gu_analysis,
                    jeonse_ratio=jeonse_ratio,
                    candidates=candidates,
                    budget=budget,
                    strategy=strategy,
                )
            st.markdown(report)
            st.session_state["report"] = report

        if "report" in st.session_state:
            st.download_button(
                "📥 리포트 다운로드",
                st.session_state["report"],
                file_name=f"투자리포트_{datetime.now().strftime('%Y%m%d')}.md",
                mime="text/markdown",
            )

        st.divider()
        st.subheader("💬 AI에게 질문하기")
        question = st.text_input("부동산 투자 관련 질문을 입력하세요")
        if question:
            context = f"시장요약: {summary}\n구별분석 상위5: {gu_analysis.head().to_string() if not gu_analysis.empty else '없음'}"
            with st.spinner("답변 생성 중..."):
                answer = ask_advisor(question, context)
            st.markdown(answer)

else:
    with tab1:
        st.info("👈 사이드바에서 '실거래가 수집 및 분석' 버튼을 눌러주세요.")
    with tab2:
        st.info("실거래가 데이터를 먼저 수집해주세요.")
    with tab3:
        st.info("실거래가 데이터를 먼저 수집해주세요.")
    with tab4:
        st.info("실거래가 데이터를 먼저 수집해주세요.")
    with tab7:
        st.info("실거래가 데이터를 먼저 수집해주세요.")

    st.markdown("""
    ### 사용 방법
    1. **매물 타입** 선택 (아파트, 빌라, 단독/다가구, 오피스텔)
    2. **투자 가용 자산** 입력 → **투자 전략** 선택
    3. **'실거래가 수집 및 분석'** 클릭 → 과거 거래 데이터 분석
    4. **'현재 매물'** 탭 → 네이버 부동산 실시간 매물 검색
    5. **'AI 리포트'** 탭 → 맞춤형 투자 분석 리포트 생성
    """)
