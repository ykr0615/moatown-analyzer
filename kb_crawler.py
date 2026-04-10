"""KB부동산 시세 및 단지 정보 크롤링 모듈

Chrome에서 KB부동산 로그인 시 → 인증 포함 KB시세 조회 가능
비로그인 시 → 단지 기본 정보만 조회 가능
"""
import requests
import time
import base64
import json
import os
import pandas as pd
from datetime import datetime

try:
    import browser_cookie3
    HAS_BROWSER_COOKIE = True
except ImportError:
    HAS_BROWSER_COOKIE = False

try:
    from Crypto.PublicKey import RSA
    from Crypto.Cipher import PKCS1_v1_5
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

BASE_URL = "https://api.kbland.kr"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://kbland.kr/",
    "Origin": "https://kbland.kr",
    "WebService": "1",
}

KB_PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1b14efejHNAhrqD5jnhX
0Xtl0IsnYrNKNOCCqVAKADwmb3jszFOMJQrOhJpsqvp/l5gdxBRlwYyXU/MZ1G7j
T+yQZGqEkzi3r4azaiudsoWl7uG5lkwNQrbzcMacMPT11ahjfzNr4JRo0nBiTt1D
JVcfyoxr07mjxIOuqOf/ACNtrtdFQeJxHMGUsC5abzGVtVVUmQZcKUa/WbVP6/NP
WqHxBbqOEU5nHutQbDwJ7M/GvHwTxSyiqct2UKPj/W4PdHAN8aBy8hLT5Twm/krS
BCBZu4ehVHNO4V1OHBjPovY+NLpfLI/CQbBjo/JMxLXgarrV8kEPyVXdw1hDNQpr
oQIDAQAB
-----END PUBLIC KEY-----"""

SEOUL_GU_CODES_KB = {
    "강남구": "1168000000", "강동구": "1174000000", "강북구": "1130500000",
    "강서구": "1150000000", "관악구": "1162000000", "광진구": "1121500000",
    "구로구": "1153000000", "금천구": "1154500000", "노원구": "1135000000",
    "도봉구": "1132000000", "동대문구": "1123000000", "동작구": "1159000000",
    "마포구": "1144000000", "서대문구": "1141000000", "서초구": "1165000000",
    "성동구": "1120000000", "성북구": "1129000000", "송파구": "1171000000",
    "양천구": "1147000000", "영등포구": "1156000000", "용산구": "1117000000",
    "은평구": "1138000000", "종로구": "1111000000", "중구": "1114000000",
    "중랑구": "1126000000",
}

PROPERTY_TYPE_MAP = {
    "아파트": "01",
    "연립다세대(빌라)": "02",
    "오피스텔": "03",
}


def _make_auth(access_token, timestamp):
    """KB 인증 헤더 생성 (RSA 암호화)"""
    if not HAS_CRYPTO or not access_token:
        return None
    plain = base64.b64encode(f"{access_token}:{timestamp}".encode()).decode()
    key = RSA.import_key(KB_PUBLIC_KEY)
    cipher = PKCS1_v1_5.new(key)
    return base64.b64encode(cipher.encrypt(plain.encode())).decode()


def _get_kb_token():
    """Chrome 브라우저에서 KB 로그인 토큰 가져오기"""
    if not HAS_BROWSER_COOKIE:
        return None
    try:
        cj = browser_cookie3.chrome(domain_name='.kbland.kr')
        cookies = {c.name: c.value for c in cj}
        return cookies.get('accessToken_', None)
    except Exception:
        return None


def _request(path, params=None, max_retries=2):
    """KB API 요청 (인증 자동 처리)"""
    token = _get_kb_token()

    for attempt in range(max_retries):
        try:
            time.sleep(0.3)
            headers = dict(HEADERS)

            if token and HAS_CRYPTO:
                ts = datetime.now().strftime('%Y%m%d%I%M%S') + f'{attempt:03d}'
                auth = _make_auth(token, ts)
                if auth:
                    headers['Authorization'] = f'bearer {auth}'
                    headers['timestamp'] = ts

            r = requests.get(
                f"{BASE_URL}{path}",
                params=params,
                headers=headers,
                timeout=30,
            )
            if r.status_code == 200:
                data = r.json()
                header = data.get("dataHeader", {})
                if header.get("resultCode") == "10000":
                    return data.get("dataBody", {})
        except requests.RequestException:
            pass
    return None


def is_logged_in():
    """KB부동산 로그인 상태 확인"""
    token = _get_kb_token()
    return bool(token and HAS_CRYPTO)


def get_dong_complexes(dong_code, property_type="아파트"):
    """특정 동의 단지 목록 조회"""
    body = _request(
        "/land-complex/complexComm/hscmList",
        {"법정동코드": dong_code},
    )
    if not body or "data" not in body:
        return []

    type_code = PROPERTY_TYPE_MAP.get(property_type, "01")
    return [c for c in body["data"] if c.get("매물종별구분") == type_code]


def get_complex_detail(complex_no):
    """단지 상세 정보 조회"""
    body = _request(
        "/land-complex/complex/brif",
        {"단지기본일련번호": str(complex_no)},
    )
    if body and "data" in body:
        return body["data"]
    return None


def get_complex_price(complex_no, area_id=None):
    """단지 KB시세 조회 (로그인 필요)

    Returns:
        dict with keys: 매매거래금액, 전세거래금액, 매매평균가, 전세평균가 등
    """
    params = {"단지기본일련번호": str(complex_no)}
    if area_id:
        params["면적일련번호"] = str(area_id)

    body = _request("/land-price/price/BasePrcInfoNew", params)
    if not body or "data" not in body:
        return None

    data = body["data"]
    result = {
        "매매건수": data.get("매매건수", 0),
        "전세건수": data.get("전세건수", 0),
        "월세건수": data.get("월세건수", 0),
        "매물매매평균가": data.get("매물매매평균가"),
        "매물전세평균가": data.get("매물전세평균가"),
    }

    # 시세 배열에서 KB시세 추출
    sise_list = data.get("시세", [])
    if sise_list:
        s = sise_list[0]
        result.update({
            "KB매매평균가": s.get("매매평균가"),
            "KB매매상한가": s.get("매매상한가"),
            "KB매매하한가": s.get("매매하한가"),
            "KB전세평균가": s.get("전세평균가"),
            "KB전세상한가": s.get("전세상한가"),
            "KB전세하한가": s.get("전세하한가"),
            "매매거래금액": s.get("매매거래금액"),
            "전세거래금액": s.get("전세거래금액"),
            "시세기준일": s.get("시세기준년월일"),
            "매매계약일": s.get("매매계약종료년월일"),
            "전세계약일": s.get("전세계약종료년월일"),
        })

    return result


def get_dong_ho_list(complex_no):
    """단지 동/호별 정보 (면적, 공시지가 등)"""
    body = _request(
        "/land-complex/complex/dongHoList",
        {"단지기본일련번호": str(complex_no)},
    )
    if body and "data" in body:
        return body["data"]
    return []


def crawl_area_kb(gu_name, dong_name=None, property_type="아파트"):
    """특정 구/동의 KB 데이터 수집"""
    gu_code = SEOUL_GU_CODES_KB.get(gu_name)
    if not gu_code:
        print(f"알 수 없는 구: {gu_name}")
        return pd.DataFrame()

    logged_in = is_logged_in()
    if logged_in:
        print(f"[KB] 로그인 상태 - KB시세 포함 조회")
    else:
        print(f"[KB] 비로그인 상태 - 단지 기본 정보만 조회 (Chrome에서 kbland.kr 로그인하면 시세 포함)")

    # 구 전체 단지 목록
    body = _request("/land-complex/complexComm/hscmList", {"법정동코드": gu_code})
    if not body or "data" not in body:
        print(f"  {gu_name} 단지 조회 실패")
        return pd.DataFrame()

    type_code = PROPERTY_TYPE_MAP.get(property_type, "01")
    all_complexes = [c for c in body["data"] if c.get("매물종별구분") == type_code]
    print(f"[{gu_name}] {property_type} {len(all_complexes)}개 단지 조회 시작...")

    results = []
    for i, c in enumerate(all_complexes):
        complex_no = c.get("단지기본일련번호")
        name = c.get("단지명", "")

        if (i + 1) % 20 == 0 or i == 0:
            print(f"  [{i+1}/{len(all_complexes)}] 진행 중...")

        # 상세 정보
        detail = get_complex_detail(complex_no)

        row = {
            "구": gu_name,
            "단지명": name,
            "매물타입": property_type,
            "법정동코드": c.get("법정동코드", ""),
        }

        if detail:
            row.update({
                "준공년월": detail.get("준공년월", ""),
                "총세대수": detail.get("총세대수", 0),
                "총동수": detail.get("총동수", 0),
                "최소전용면적": detail.get("최소전용면적", ""),
                "최대전용면적": detail.get("최대전용면적", ""),
                "재건축여부": detail.get("재건축여부", "0"),
            })

        # 로그인 시 KB시세 + 동호 정보
        if logged_in:
            # 면적일련번호 가져오기 (첫번째 동/호에서)
            dong_ho = get_dong_ho_list(complex_no)
            area_id = dong_ho[0].get("면적일련번호") if dong_ho else None

            if area_id:
                # 공시지가 정보 (동호 첫번째에서)
                row["현재공시지가"] = dong_ho[0].get("현재공시지가")
                row["공시지가증가율"] = dong_ho[0].get("공시지가증가율")

            price = get_complex_price(complex_no, area_id)
            if price:
                row.update({
                    "KB매매평균가": price.get("KB매매평균가"),
                    "KB전세평균가": price.get("KB전세평균가"),
                    "매매거래금액": price.get("매매거래금액"),
                    "전세거래금액": price.get("전세거래금액"),
                    "시세기준일": price.get("시세기준일"),
                    "매매건수": price.get("매매건수", 0),
                    "전세건수": price.get("전세건수", 0),
                })

        results.append(row)

    df = pd.DataFrame(results)
    if not df.empty:
        for col in ["KB매매평균가", "KB전세평균가", "매매거래금액", "전세거래금액", "현재공시지가"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "KB매매평균가" in df.columns and "KB전세평균가" in df.columns:
            mask = (df["KB매매평균가"] > 0) & (df["KB전세평균가"] > 0)
            df.loc[mask, "전세가율"] = (df.loc[mask, "KB전세평균가"] / df.loc[mask, "KB매매평균가"] * 100).round(1)
            df.loc[mask, "갭(매매-전세)"] = df.loc[mask, "KB매매평균가"] - df.loc[mask, "KB전세평균가"]

        if "매매거래금액" in df.columns and "전세거래금액" in df.columns:
            mask2 = (df["매매거래금액"] > 0) & (df["전세거래금액"] > 0)
            df.loc[mask2, "실거래전세가율"] = (df.loc[mask2, "전세거래금액"] / df.loc[mask2, "매매거래금액"] * 100).round(1)

    print(f"  완료! {len(df)}개 단지")
    return df


if __name__ == "__main__":
    print(f"로그인 상태: {'✓' if is_logged_in() else '✗'}")
    print(f"browser_cookie3: {'✓' if HAS_BROWSER_COOKIE else '✗'}")
    print(f"pycryptodome: {'✓' if HAS_CRYPTO else '✗'}")

    print("\n=== 반포동 빌라 KB시세 ===")
    complexes = get_dong_complexes("1165010700", "연립다세대(빌라)")
    print(f"단지 수: {len(complexes)}")

    for c in complexes[:3]:
        name = c.get("단지명", "?")
        cno = c["단지기본일련번호"]
        dong_ho = get_dong_ho_list(cno)
        area_id = dong_ho[0]["면적일련번호"] if dong_ho else None
        price = get_complex_price(cno, area_id)

        print(f"\n  {name}:")
        if price:
            for k, v in price.items():
                if v is not None and v != 0:
                    print(f"    {k}: {v}")
