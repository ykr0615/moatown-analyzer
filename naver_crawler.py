"""네이버 부동산 현재 매물 크롤링 모듈

Rate limit 대응 전략:
1. 요청 간 2초 딜레이
2. 429 발생 시 최대 60초까지 점진적 대기
3. 로컬 캐싱으로 중복 요청 방지
4. 세션 기반 요청으로 쿠키 유지
"""
import requests
import time
import json
import os
import hashlib
import pandas as pd
from datetime import datetime, timedelta
from config import SEOUL_GU_CODES

CACHE_DIR = os.path.join(os.path.dirname(__file__), ".cache")
CACHE_TTL_HOURS = 1  # 캐시 유효시간

REAL_ESTATE_TYPES = {
    "아파트": "APT",
    "연립다세대(빌라)": "VL",
    "오피스텔": "OPST",
    "단독/다가구": "DDDGG",
}

TRADE_TYPES = {
    "매매": "A1",
    "전세": "B1",
    "월세": "B2",
}

SEOUL_GU_CORTAR = {
    "강남구": "1168000000",
    "강동구": "1174000000",
    "강북구": "1130500000",
    "강서구": "1150000000",
    "관악구": "1162000000",
    "광진구": "1121500000",
    "구로구": "1153000000",
    "금천구": "1154500000",
    "노원구": "1135000000",
    "도봉구": "1132000000",
    "동대문구": "1123000000",
    "동작구": "1159000000",
    "마포구": "1144000000",
    "서대문구": "1141000000",
    "서초구": "1165000000",
    "성동구": "1120000000",
    "성북구": "1129000000",
    "송파구": "1171000000",
    "양천구": "1147000000",
    "영등포구": "1156000000",
    "용산구": "1117000000",
    "은평구": "1138000000",
    "종로구": "1111000000",
    "중구": "1114000000",
    "중랑구": "1126000000",
}


def _get_session():
    """세션 생성 (쿠키 유지)"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Referer": "https://new.land.naver.com/",
        "sec-ch-ua": '"Chromium";v="131"',
        "sec-ch-ua-platform": '"macOS"',
    })
    return session


def _cache_key(url):
    """URL을 캐시 키로 변환"""
    return hashlib.md5(url.encode()).hexdigest()


def _get_cache(url):
    """캐시에서 데이터 가져오기"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(CACHE_DIR, f"{_cache_key(url)}.json")

    if not os.path.exists(cache_file):
        return None

    mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
    if datetime.now() - mtime > timedelta(hours=CACHE_TTL_HOURS):
        return None

    with open(cache_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _set_cache(url, data):
    """데이터를 캐시에 저장"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(CACHE_DIR, f"{_cache_key(url)}.json")
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def _safe_request(session, url, max_retries=3):
    """Rate limit 대응 요청 (캐시 + 점진적 대기)"""
    # 캐시 확인
    cached = _get_cache(url)
    if cached is not None:
        return cached

    for attempt in range(max_retries):
        try:
            time.sleep(2)  # 기본 딜레이
            response = session.get(url, timeout=15)

            if response.status_code == 200:
                try:
                    data = response.json()
                    _set_cache(url, data)
                    return data
                except ValueError:
                    return None

            elif response.status_code == 429:
                wait = min(10 * (2 ** attempt), 60)
                print(f"  ⏳ Rate limit, {wait}초 대기 중... ({attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                return None

        except requests.RequestException as e:
            print(f"  요청 실패: {e}")
            return None

    print("  ❌ Rate limit 지속 - 나중에 다시 시도해주세요")
    return None


def get_dong_list(session, gu_cortar_no):
    """구 내 동 목록 조회"""
    url = f"https://new.land.naver.com/api/regions/list?cortarNo={gu_cortar_no}"
    data = _safe_request(session, url)
    if data and "regionList" in data:
        return [
            {"name": r["cortarName"], "code": r["cortarNo"]}
            for r in data["regionList"]
        ]
    return []


def get_listings(session, cortar_no, real_estate_type="VL", trade_type="A1", page=1):
    """특정 동의 현재 매물 조회"""
    url = (
        f"https://new.land.naver.com/api/articles?"
        f"cortarNo={cortar_no}&realEstateType={real_estate_type}"
        f"&tradeType={trade_type}&page={page}&sameAddressGroup=true"
    )
    data = _safe_request(session, url)

    if not data or "articleList" not in data:
        return [], False

    articles = []
    for a in data["articleList"]:
        articles.append({
            "매물명": a.get("articleName", ""),
            "매물타입": a.get("realEstateTypeName", ""),
            "거래타입": a.get("tradeTypeName", ""),
            "호가": a.get("dealOrWarrantPrc", ""),
            "전용면적": a.get("area2", ""),
            "층정보": a.get("floorInfo", ""),
            "방향": a.get("direction", ""),
            "확인일자": a.get("articleConfirmYmd", ""),
            "매물설명": a.get("articleFeatureDesc", ""),
            "중개사": a.get("realtorName", ""),
            "태그": ", ".join(a.get("tagList", [])),
            "articleNo": a.get("articleNo", ""),
        })

    has_more = len(data["articleList"]) >= 20
    return articles, has_more


def get_all_listings_for_dong(session, cortar_no, real_estate_type="VL", trade_type="A1"):
    """특정 동의 모든 매물 조회 (페이지네이션)"""
    all_articles = []
    page = 1
    while True:
        articles, has_more = get_listings(session, cortar_no, real_estate_type, trade_type, page)
        all_articles.extend(articles)
        if not has_more or page >= 10:
            break
        page += 1
    return all_articles


def crawl_gu_listings(gu_name, property_type="연립다세대(빌라)", trade_type_name="매매"):
    """특정 구의 전체 매물 수집"""
    session = _get_session()
    gu_cortar = SEOUL_GU_CORTAR.get(gu_name)
    if not gu_cortar:
        print(f"알 수 없는 구: {gu_name}")
        return pd.DataFrame()

    re_type = REAL_ESTATE_TYPES.get(property_type, "VL")
    tr_type = TRADE_TYPES.get(trade_type_name, "A1")

    print(f"[{gu_name}] 동 목록 조회 중...")
    dongs = get_dong_list(session, gu_cortar)
    if not dongs:
        print(f"  동 목록 조회 실패 (rate limit일 수 있음)")
        return pd.DataFrame()

    all_articles = []
    for i, dong in enumerate(dongs):
        print(f"  [{i+1}/{len(dongs)}] {dong['name']} 매물 수집 중...")
        articles = get_all_listings_for_dong(session, dong["code"], re_type, tr_type)
        for a in articles:
            a["구"] = gu_name
            a["동"] = dong["name"]
        all_articles.extend(articles)
        print(f"    → {len(articles)}건")

    return _to_dataframe(all_articles)


def crawl_dong_listings(gu_name, dong_name, property_type="연립다세대(빌라)", trade_types=None):
    """특정 동의 매물 수집 (매매+전세 동시)"""
    if trade_types is None:
        trade_types = ["매매", "전세"]

    session = _get_session()
    gu_cortar = SEOUL_GU_CORTAR.get(gu_name)
    if not gu_cortar:
        return pd.DataFrame()

    re_type = REAL_ESTATE_TYPES.get(property_type, "VL")

    # 동 코드 찾기
    dongs = get_dong_list(session, gu_cortar)
    dong_code = None
    for d in dongs:
        if dong_name in d["name"]:
            dong_code = d["code"]
            break

    if not dong_code:
        dong_names = [d["name"] for d in dongs]
        print(f"{dong_name} 코드를 찾을 수 없습니다. 동 목록: {dong_names}")
        return pd.DataFrame()

    all_articles = []
    for trade_name in trade_types:
        tr_type = TRADE_TYPES.get(trade_name, "A1")
        print(f"[{gu_name} {dong_name}] {property_type} {trade_name} 매물 수집 중...")
        articles = get_all_listings_for_dong(session, dong_code, re_type, tr_type)
        for a in articles:
            a["구"] = gu_name
            a["동"] = dong_name
        all_articles.extend(articles)
        print(f"  → {len(articles)}건")

    return _to_dataframe(all_articles)


def _to_dataframe(articles):
    """매물 리스트를 DataFrame으로 변환"""
    df = pd.DataFrame(articles)
    if not df.empty:
        df["호가(만원)"] = df["호가"].apply(_parse_price)
        df["전용면적"] = pd.to_numeric(df["전용면적"], errors="coerce")
        df["평"] = (df["전용면적"] / 3.306).round(1)
    return df


def clear_cache():
    """캐시 전체 삭제"""
    import shutil
    if os.path.exists(CACHE_DIR):
        shutil.rmtree(CACHE_DIR)
        print("캐시 삭제 완료")


def _parse_price(price_str):
    """네이버 부동산 호가 문자열을 만원 단위 숫자로 변환
    예: '5억 2,000' -> 52000, '8,500' -> 8500, '12억' -> 120000
    """
    if not price_str or not isinstance(price_str, str):
        return 0

    price_str = price_str.replace(",", "").replace(" ", "")
    total = 0

    if "억" in price_str:
        parts = price_str.split("억")
        try:
            total += int(parts[0]) * 10000
        except ValueError:
            return 0
        if len(parts) > 1 and parts[1]:
            try:
                total += int(parts[1])
            except ValueError:
                pass
    else:
        try:
            total = int(price_str)
        except ValueError:
            return 0

    return total


if __name__ == "__main__":
    print("=== 반포동 빌라 매물 테스트 ===")
    df = crawl_dong_listings("서초구", "반포", property_type="연립다세대(빌라)")
    if not df.empty:
        print(f"\n총 {len(df)}건 매물")
        print(df[["매물명", "거래타입", "호가", "호가(만원)", "전용면적", "층정보"]].head(20).to_string())
    else:
        print("매물 수집 실패 (rate limit일 수 있음, 1~2분 후 재시도)")
