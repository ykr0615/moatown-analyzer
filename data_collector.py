"""국토교통부 실거래가 API 데이터 수집 모듈"""
from typing import Dict, List, Optional
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
from config import DATA_GO_KR_API_KEY, SEOUL_GU_CODES


# 매물 타입별 API 엔드포인트
PROPERTY_APIS = {
    "아파트": {
        "trade": "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
        "rent": "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
    },
    "연립다세대(빌라)": {
        "trade": "https://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
        "rent": "https://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
    },
    "단독/다가구": {
        "trade": "https://apis.data.go.kr/1613000/RTMSDataSvcSHTradeDev/getRTMSDataSvcSHTradeDev",
        "rent": "https://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
    },
    "오피스텔": {
        "trade": "https://apis.data.go.kr/1613000/RTMSDataSvcOffiTradeDev/getRTMSDataSvcOffiTradeDev",
        "rent": "https://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
    },
}

PROPERTY_TYPES = list(PROPERTY_APIS.keys())


def _api_request(url: str, gu_code: str, deal_ymd: str) -> Optional[str]:
    """공통 API 요청"""
    full_url = f"{url}?serviceKey={DATA_GO_KR_API_KEY}&LAWD_CD={gu_code}&DEAL_YMD={deal_ymd}&pageNo=1&numOfRows=9999"
    try:
        response = requests.get(full_url, timeout=10)
        response.raise_for_status()
        if "<resultCode>000</resultCode>" not in response.text:
            return None
        return response.text
    except requests.RequestException:
        return None


def fetch_trade(gu_code: str, deal_ymd: str, property_type: str = "아파트") -> List[dict]:
    """매매 실거래가 조회 (매물타입 지원)"""
    api = PROPERTY_APIS.get(property_type)
    if not api:
        return []

    xml_text = _api_request(api["trade"], gu_code, deal_ymd)
    if not xml_text:
        return []

    return _parse_trade_xml(xml_text, property_type)


def fetch_rent(gu_code: str, deal_ymd: str, property_type: str = "아파트") -> List[dict]:
    """전월세 실거래가 조회 (매물타입 지원)"""
    api = PROPERTY_APIS.get(property_type)
    if not api:
        return []

    xml_text = _api_request(api["rent"], gu_code, deal_ymd)
    if not xml_text:
        return []

    return _parse_rent_xml(xml_text, property_type)


def check_api_available(property_type: str) -> bool:
    """해당 매물타입 API 사용 가능 여부 확인"""
    api = PROPERTY_APIS.get(property_type)
    if not api:
        return False
    xml_text = _api_request(api["trade"], "11650", "202603")
    return xml_text is not None


def _get_name_tag(property_type: str) -> str:
    """매물타입별 이름 태그"""
    if property_type == "아파트":
        return "aptNm"
    elif property_type == "오피스텔":
        return "offiNm"
    else:
        return "aptNm"


def _parse_trade_xml(xml_text: str, property_type: str = "아파트") -> List[dict]:
    """매매 실거래가 XML 파싱"""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    name_tag = _get_name_tag(property_type)
    items = root.findall(".//item")
    results = []

    for item in items:
        def get_text(tag):
            el = item.find(tag)
            return el.text.strip() if el is not None and el.text else ""

        results.append({
            "매물명": get_text(name_tag),
            "매물타입": property_type,
            "법정동": get_text("umdNm"),
            "전용면적": get_text("excluUseAr"),
            "거래금액": get_text("dealAmount").replace(",", ""),
            "건축년도": get_text("buildYear"),
            "층": get_text("floor"),
            "거래년도": get_text("dealYear"),
            "거래월": get_text("dealMonth"),
            "거래일": get_text("dealDay"),
        })

    return results


def _parse_rent_xml(xml_text: str, property_type: str = "아파트") -> List[dict]:
    """전월세 실거래가 XML 파싱"""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    name_tag = _get_name_tag(property_type)
    items = root.findall(".//item")
    results = []

    for item in items:
        def get_text(tag):
            el = item.find(tag)
            return el.text.strip() if el is not None and el.text else ""

        results.append({
            "매물명": get_text(name_tag),
            "매물타입": property_type,
            "법정동": get_text("umdNm"),
            "전용면적": get_text("excluUseAr"),
            "보증금": get_text("deposit").replace(",", ""),
            "월세": get_text("monthlyRent").replace(",", ""),
            "건축년도": get_text("buildYear"),
            "층": get_text("floor"),
            "거래년도": get_text("dealYear"),
            "거래월": get_text("dealMonth"),
            "거래일": get_text("dealDay"),
        })

    return results


def collect_seoul_trades(months: int = 6, property_types: List[str] = None) -> pd.DataFrame:
    """서울 전체 구 매매 실거래가 수집"""
    if property_types is None:
        property_types = ["아파트"]

    all_trades = []
    now = datetime.now()

    deal_months = []
    for i in range(months):
        target = now - timedelta(days=30 * i)
        deal_months.append(target.strftime("%Y%m"))

    total = len(SEOUL_GU_CODES) * len(deal_months) * len(property_types)
    count = 0

    for ptype in property_types:
        for gu_name, gu_code in SEOUL_GU_CODES.items():
            for deal_ymd in deal_months:
                count += 1
                print(f"[{count}/{total}] {ptype} {gu_name} {deal_ymd} 수집 중...")
                trades = fetch_trade(gu_code, deal_ymd, ptype)
                for t in trades:
                    t["구"] = gu_name
                all_trades.extend(trades)

    df = pd.DataFrame(all_trades)
    if not df.empty:
        df["거래금액"] = pd.to_numeric(df["거래금액"], errors="coerce")
        df["전용면적"] = pd.to_numeric(df["전용면적"], errors="coerce")
        df["층"] = pd.to_numeric(df["층"], errors="coerce")
        df["평"] = (df["전용면적"] / 3.306).round(1)
        df["평당가격"] = (df["거래금액"] / df["평"]).round(0)

    return df


def collect_seoul_rents(months: int = 6, property_types: List[str] = None) -> pd.DataFrame:
    """서울 전체 구 전월세 실거래가 수집"""
    if property_types is None:
        property_types = ["아파트"]

    all_rents = []
    now = datetime.now()

    deal_months = []
    for i in range(months):
        target = now - timedelta(days=30 * i)
        deal_months.append(target.strftime("%Y%m"))

    total = len(SEOUL_GU_CODES) * len(deal_months) * len(property_types)
    count = 0

    for ptype in property_types:
        for gu_name, gu_code in SEOUL_GU_CODES.items():
            for deal_ymd in deal_months:
                count += 1
                print(f"[{count}/{total}] {ptype} 전월세 {gu_name} {deal_ymd} 수집 중...")
                rents = fetch_rent(gu_code, deal_ymd, ptype)
                for r in rents:
                    r["구"] = gu_name
                all_rents.extend(rents)

    df = pd.DataFrame(all_rents)
    if not df.empty:
        df["보증금"] = pd.to_numeric(df["보증금"], errors="coerce")
        df["월세"] = pd.to_numeric(df["월세"], errors="coerce")
        df["전용면적"] = pd.to_numeric(df["전용면적"], errors="coerce")

    return df


if __name__ == "__main__":
    print("=== API 사용 가능 여부 확인 ===")
    for ptype in PROPERTY_TYPES:
        available = check_api_available(ptype)
        status = "✓ 사용가능" if available else "✗ 신청필요"
        print(f"  {ptype}: {status}")
