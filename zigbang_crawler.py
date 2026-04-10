"""직방(Zigbang) 현재 매물 크롤링 모듈

네이버 부동산 대비 장점:
- Rate limit 거의 없음
- 매물 상세 정보 풍부 (관리비, 주차, 방향, 설명 등)
- API가 안정적
"""
import requests
import time
import pandas as pd

HEADERS = {"User-Agent": "Mozilla/5.0"}

# 서울 구별 geohash (여러 geohash로 넓은 범위 커버)
SEOUL_GU_GEOHASH = {
    "강남구": ["wydm6", "wydm7", "wydmd"],
    "강동구": ["wydme", "wydmk", "wydms"],
    "강북구": ["wydmx", "wydmr"],
    "강서구": ["wydhp", "wydhr", "wydjn"],
    "관악구": ["wydm0", "wydm1", "wydm2"],
    "광진구": ["wydm6", "wydmd"],
    "구로구": ["wydhx", "wydhz"],
    "금천구": ["wydhw", "wydhx"],
    "노원구": ["wydqp", "wydqn"],
    "도봉구": ["wydqn", "wydqj"],
    "동대문구": ["wydm5", "wydmh"],
    "동작구": ["wydm2", "wydm3"],
    "마포구": ["wydjx", "wydjz"],
    "서대문구": ["wydjw", "wydjx"],
    "서초구": ["wydm3", "wydm4", "wydm6"],
    "성동구": ["wydm4", "wydm5"],
    "성북구": ["wydm5", "wydmh"],
    "송파구": ["wydmk", "wydme", "wydms"],
    "양천구": ["wydhz", "wydj8"],
    "영등포구": ["wydjr", "wydjx"],
    "용산구": ["wydm9", "wydm3"],
    "은평구": ["wydjz", "wydjy"],
    "종로구": ["wydm2", "wydm9"],
    "중구": ["wydm1", "wydm3"],
    "중랑구": ["wydm7", "wydmh"],
}

# 매물 타입 매핑
PROPERTY_TYPE_MAP = {
    "아파트": "apt",
    "연립다세대(빌라)": "villa",
    "오피스텔": "officetel",
    "원룸": "oneroom",
}


def get_item_ids(geohashes, property_type="villa", sales_types=None, size=200):
    """지역 내 매물 ID 목록 조회 (여러 geohash 지원)"""
    if sales_types is None:
        sales_types = ["매매", "전세"]

    if isinstance(geohashes, str):
        geohashes = [geohashes]

    zb_type = PROPERTY_TYPE_MAP.get(property_type, "villa")
    url = f"https://apis.zigbang.com/v2/items/{zb_type}"

    all_ids = set()
    for gh in geohashes:
        try:
            r = requests.get(url, params={
                "domain": "zigbang", "geohash": gh,
                "depositMin": 0, "rentMin": 0,
                "salesTypes": sales_types,
                "page": 1, "size": size,
            }, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                items = r.json().get("items", [])
                all_ids.update(i["itemId"] for i in items)
        except requests.RequestException:
            pass

    return list(all_ids)


def get_item_detail(item_id):
    """매물 상세 정보 조회"""
    try:
        r = requests.get(
            f"https://apis.zigbang.com/v3/items/{item_id}",
            headers=HEADERS, timeout=10,
        )
        if r.status_code == 200:
            return r.json().get("item", r.json())
    except requests.RequestException:
        pass
    return None


def crawl_listings(gu_name, property_type="연립다세대(빌라)", sales_types=None):
    """특정 구의 현재 매물 수집

    Args:
        gu_name: 구 이름 (예: "서초구")
        property_type: 매물 타입
        sales_types: 거래 타입 리스트 (예: ["매매", "전세"])
    """
    if sales_types is None:
        sales_types = ["매매", "전세"]

    geohashes = SEOUL_GU_GEOHASH.get(gu_name)
    if not geohashes:
        print(f"알 수 없는 구: {gu_name}")
        return pd.DataFrame()

    # 1. 매물 ID 목록 수집 (여러 geohash 조합)
    print(f"[직방] {gu_name} {property_type} 매물 검색 중...")
    item_ids = get_item_ids(geohashes, property_type, sales_types)
    print(f"  매물 ID: {len(item_ids)}건")

    if not item_ids:
        return pd.DataFrame()

    # 2. 상세 정보 수집
    results = []
    total = len(item_ids)

    for i, iid in enumerate(item_ids):
        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{total}] 상세 조회 중...")
            time.sleep(0.5)

        detail = get_item_detail(iid)
        if not detail:
            continue

        price = detail.get("price", {})
        area = detail.get("area", {})
        floor_info = detail.get("floor", {})
        addr = detail.get("addressOrigin", {})
        manage = detail.get("manageCost", {})

        row = {
            "매물ID": iid,
            "매물타입": detail.get("serviceType", ""),
            "거래타입": detail.get("salesType", ""),
            "매물명": detail.get("title", ""),
            "보증금(만원)": price.get("deposit", 0),
            "월세(만원)": price.get("rent", 0),
            "전용면적": area.get("전용면적M2", 0),
            "구": addr.get("local2", ""),
            "동": addr.get("local3", ""),
            "지번주소": detail.get("jibunAddress", ""),
            "층": floor_info.get("floor", ""),
            "총층": floor_info.get("allFloors", ""),
            "방향": detail.get("roomDirection", ""),
            "방타입": detail.get("roomType", ""),
            "관리비(만원)": manage.get("amount", 0),
            "주차": detail.get("parkingAvailableText", ""),
            "엘리베이터": detail.get("elevator", False),
            "입주가능일": detail.get("moveinDate", ""),
            "승인일(준공)": detail.get("approveDate", ""),
            "매물설명": detail.get("description", "")[:100],
            "등록일": detail.get("updatedAt", "")[:10],
            "중개사": detail.get("agent", {}).get("agentTitle", ""),
        }

        results.append(row)

    df = pd.DataFrame(results)

    if not df.empty:
        df["전용면적"] = pd.to_numeric(df["전용면적"], errors="coerce")
        df["평"] = (df["전용면적"] / 3.306).round(1)
        df["보증금(만원)"] = pd.to_numeric(df["보증금(만원)"], errors="coerce")

        # 구 필터링 (geohash가 넓어서 다른 구 매물이 포함될 수 있음)
        if gu_name:
            target_gu = gu_name.replace("구", "")
            df = df[df["구"].str.contains(target_gu, na=False)].copy()

    print(f"  ✅ {len(df)}건 수집 완료")
    return df


if __name__ == "__main__":
    print("=== 서초구 빌라 매물 (직방) ===\n")
    df = crawl_listings("서초구", "연립다세대(빌라)", ["매매", "전세"])

    if not df.empty:
        # 반포동만 필터
        banpo = df[df["동"].str.contains("반포", na=False)]
        print(f"\n반포동 매물: {len(banpo)}건\n")

        for _, r in banpo.head(15).iterrows():
            price = f"보증금 {r['보증금(만원)']:,.0f}" if r['보증금(만원)'] > 0 else ""
            rent = f" / 월세 {r['월세(만원)']:,.0f}" if r['월세(만원)'] > 0 else ""
            print(f"  [{r['거래타입']}] {r['매물명'][:25]:25} | {price}{rent} | {r['전용면적']}㎡({r['평']}평) | {r['층']}층/{r['총층']}층 | {r['동']}")
    else:
        print("매물 없음")
