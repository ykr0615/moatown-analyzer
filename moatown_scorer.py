"""모아타운/모아주택 선정 가능성 예측 스코어링 엔진

서울시 모아타운 선정 기준을 역설계하여,
미선정 지역 중 향후 선정 가능성이 높은 곳을 예측합니다.

스코어링 요소:
1. 노후도 (30년+ 건물 비율) - 필수요건 50%+
2. 면적 적합성 (10만㎡ 이내)
3. 사업성 (공시지가 낮을수록 유리)
4. 기반시설 부족도 (부족할수록 유리)
5. 용적률 갭 (현재 vs 허용 차이 클수록 유리)
6. 주민동의 용이성 (교회/대형상가 없을수록 유리)
7. 인접 정비사업 효과 (주변 재개발/재건축 있으면 유리)
8. 정책 방향 (정부 선정 속도 추이 반영)
"""
import pandas as pd
import numpy as np
from datetime import datetime


# ============================================================
# 1. 기선정 모아타운 데이터 (학습/검증용)
# ============================================================
DESIGNATED_MOATOWNS = [
    # (구, 동, 대표지번, 선정일) - HWP 파싱 기반 119개소 (2026.1월 기준)
    # 강서구
    ("강서구", "화곡동", "1087", "22.01.20"), ("강서구", "화곡동", "354", "22.01.20"),
    ("강서구", "화곡동", "359", "22.01.20"), ("강서구", "화곡동", "424", "22.01.20"),
    ("강서구", "등촌동", "515-44", "22.06.23"), ("강서구", "화곡동", "1130-7", "22.06.23"),
    ("강서구", "방화동", "592-1", "22.06.23"), ("강서구", "공항동", "55-327", "22.10.27"),
    ("강서구", "화곡동", "957", "22.01.20"),
    # 중랑구
    ("중랑구", "면목동", "86-3", "22.01.20"), ("중랑구", "면목동", "44-6", "22.06.23"),
    ("중랑구", "면목동", "297-28", "22.06.23"), ("중랑구", "중화동", "4-30", "22.06.23"),
    ("중랑구", "망우동", "427-5", "22.06.23"), ("중랑구", "면목동", "152-1", "22.10.27"),
    ("중랑구", "면목동", "63-1", "22.10.27"), ("중랑구", "망우동", "354-2", "23.08.31"),
    ("중랑구", "중화동", "329-38", "23.08.31"), ("중랑구", "망우동", "474-29", "23.11.30"),
    ("중랑구", "묵동", "243-7", "23.11.30"), ("중랑구", "중화동", "299-8", "24.02.29"),
    ("중랑구", "면목동", "139-52", "24.02.22"), ("중랑구", "면목동", "453-1", "24.02.29"),
    ("중랑구", "신내동", "493-13", "24.09.12"), ("중랑구", "망우동", "509", "25.07.03"),
    ("중랑구", "면목동", "127-26", "25.10.31"), ("중랑구", "면목동", "377-4", "25.11.10"),
    # 강북구
    ("강북구", "번동", "429-114", "22.06.23"), ("강북구", "번동", "454-61", "22.06.23"),
    ("강북구", "번동", "411", "22.10.27"), ("강북구", "수유동", "52-1", "22.10.27"),
    ("강북구", "수유동", "392-9", "24.02.22"), ("강북구", "수유동", "141", "23.11.30"),
    ("강북구", "번동", "469", "24.09.30"), ("강북구", "수유동", "31-10", "24.09.30"),
    ("강북구", "미아동", "791-1134", "25.05.01"),
    # 성북구
    ("성북구", "석관동", "334-69", "22.10.27"), ("성북구", "석관동", "261-22", "22.10.27"),
    ("성북구", "종암동", "125-1", "22.11.08"), ("성북구", "정릉동", "559-43", "22.11.08"),
    ("성북구", "정릉동", "226-1", "24.10.04"), ("성북구", "정릉동", "199-1", "25.02.04"),
    ("성북구", "하월곡동", "40-107", "25.10.17"), ("성북구", "장위동", "219-15", "25.09.08"),
    ("성북구", "삼선동3가", "42-7", "25.11.17"),
    # 마포구
    ("마포구", "대흥동", "535-2", "22.06.23"), ("마포구", "성산동", "160-4", "22.06.23"),
    ("마포구", "망원동", "456-6", "22.06.23"), ("마포구", "합정동", "369", "22.10.27"),
    ("마포구", "중동", "78", "22.10.27"), ("마포구", "창전동", "46-1", "25.02.18"),
    ("마포구", "망원동", "464-1", "25.09.19"),
    # 구로구
    ("구로구", "고척동", "241", "22.06.23"), ("구로구", "구로동", "728", "22.06.23"),
    ("구로구", "개봉동", "270-38", "22.10.27"), ("구로구", "구로동", "511", "23.11.30"),
    ("구로구", "개봉동", "20", "24.09.19"),
    # 금천구
    ("금천구", "시흥동", "1005", "22.06.23"), ("금천구", "시흥동", "817", "22.06.23"),
    ("금천구", "시흥동", "922-61", "22.06.23"), ("금천구", "시흥동", "864", "22.10.27"),
    ("금천구", "시흥동", "950", "22.10.27"), ("금천구", "시흥동", "972", "25.02.27"),
    # 도봉구
    ("도봉구", "쌍문동", "524-87", "22.06.23"), ("도봉구", "쌍문동", "494-22", "22.06.23"),
    ("도봉구", "방학동", "618", "23.09.27"), ("도봉구", "쌍문동", "460", "23.09.27"),
    # 노원구
    ("노원구", "상계동", "177-66", "22.06.23"), ("노원구", "월계동", "534", "22.10.27"),
    # 강동구
    ("강동구", "둔촌동", "77-41", "22.01.20"), ("강동구", "천호동", "113-2", "22.10.27"),
    ("강동구", "천호동", "338", "24.05.23"),
    # 동작구
    ("동작구", "노량진동", "221-24", "22.10.27"), ("동작구", "사당동", "202-29", "22.10.27"),
    ("동작구", "상도동", "242", "23.09.27"), ("동작구", "상도동", "279", "24.02.22"),
    ("동작구", "동작동", "102-8", "25.03.20"),
    # 송파구
    ("송파구", "풍납동", "483-10", "22.06.23"), ("송파구", "거여동", "555", "22.06.23"),
    # 양천구
    ("양천구", "신월동", "173", "22.06.23"), ("양천구", "신월동", "102-33", "22.06.23"),
    ("양천구", "목동", "724-1", "23.07.06"), ("양천구", "목동", "231-27", "23.03.13"),
    ("양천구", "목동", "644-1", "24.07.30"),
    # 관악구
    ("관악구", "청룡동", "1535", "22.10.27"), ("관악구", "난곡동", "697-20", "24.07.12"),
    ("관악구", "신림동", "655-78", "24.04.25"),
    # 은평구
    ("은평구", "불광동", "170", "22.10.27"), ("은평구", "대조동", "89", "22.10.27"),
    ("은평구", "성현동", "1021", "23.07.06"), ("은평구", "은천동", "635-540", "23.09.27"),
    ("은평구", "은천동", "938-5", "23.09.27"), ("은평구", "응암동", "227", "25.10.27"),
    # 서대문구
    ("서대문구", "천연동", "89-16", "22.06.23"), ("서대문구", "홍제동", "322", "23.12.07"),
    ("서대문구", "홍은동", "10-18", "24.10.10"), ("서대문구", "홍은동", "11-360", "22.11.10"),
    ("서대문구", "연희동", "520", "25.02.10"),
    # 광진구
    ("광진구", "자양동", "799", "24.05.01"), ("광진구", "자양동", "649", "24.05.03"),
    ("광진구", "자양동", "681", "24.02.22"), ("광진구", "광장동", "264-1", "24.10.02"),
    ("광진구", "자양동", "772-1", "24.08.28"), ("광진구", "자양동", "226-1", "25.01.15"),
    ("광진구", "구의동", "587-58", "25.02.28"),
    # 서초구
    ("서초구", "방배동", "977", "22.01.20"), ("서초구", "양재동", "374", "23.08.31"),
    ("서초구", "양재동", "382", "23.08.31"), ("서초구", "우면동", "2", "24.03.27"),
    ("서초구", "서초동", "1506-6", "25.06.18"),
    # 성동구
    ("성동구", "마장동", "457", "22.06.23"), ("성동구", "응봉동", "265", "22.10.27"),
    ("성동구", "금호동1가", "129", "22.10.27"),
    # 영등포구
    ("영등포구", "대림동", "786", "22.10.27"),
    # 용산구
    ("용산구", "서계동", "116", "21.06.25"),
    # 종로구
    ("종로구", "구기동", "100-48", "22.06.23"), ("종로구", "숭인동", "61", "24.06.27"),
    ("종로구", "현저동", "1-5", "24.05.09"),
    # 강남구
    ("강남구", "일원동", "619-641", "22.10.27"),
    # 동대문구
    ("동대문구", "답십리동", "489", "23.12.07"),
]


# ============================================================
# 2. 서울시 동 단위 기본 데이터
# ============================================================
SEOUL_GU_LIST = [
    "강남구", "강동구", "강북구", "강서구", "관악구", "광진구",
    "구로구", "금천구", "노원구", "도봉구", "동대문구", "동작구",
    "마포구", "서대문구", "서초구", "성동구", "성북구", "송파구",
    "양천구", "영등포구", "용산구", "은평구", "종로구", "중구", "중랑구",
]

# 시군구코드 (건축물대장 API용)
SIGUNGU_CODES = {
    "종로구": "11110", "중구": "11140", "용산구": "11170",
    "성동구": "11200", "광진구": "11215", "동대문구": "11230",
    "중랑구": "11260", "성북구": "11290", "강북구": "11305",
    "도봉구": "11320", "노원구": "11350", "은평구": "11380",
    "서대문구": "11410", "마포구": "11440", "양천구": "11470",
    "강서구": "11500", "구로구": "11530", "금천구": "11545",
    "영등포구": "11560", "동작구": "11590", "관악구": "11620",
    "서초구": "11650", "강남구": "11680", "송파구": "11710",
    "강동구": "11740",
}


# ============================================================
# 3. 스코어링 엔진
# ============================================================
class MoatownScorer:
    """모아타운 선정 가능성 스코어링"""

    # 스코어 가중치 (기선정 11개 지역 검증 후 보정 완료)
    # 검증: 기선정 평균 53.2 vs 비선정 평균 41.6 (11.6점 차)
    WEIGHTS = {
        "노후도": 35,          # 가장 중요 (필수요건) - 가중치 상향
        "사업성_공시지가": 15,   # 공시지가 낮을수록 유리
        "용적률_갭": 10,        # 현재 vs 허용 차이 - 약간 하향
        "기반시설_부족도": 8,    # 도로/주차/공원 부족
        "주민동의_용이성": 18,   # 교회/상가 없을수록 유리 - 상향 (현실 반영)
        "면적_적합성": 4,       # 10만㎡ 이내
        "인접_정비사업": 5,     # 주변 재개발 있으면 가점
        "정책_모멘텀": 5,       # 정부 정책 방향
    }

    def __init__(self):
        self.building_data = None
        self.land_price_data = None
        self.designated = set()

        # 기선정 지역 등록
        for item in DESIGNATED_MOATOWNS:
            gu, dong = item[0], item[1]
            self.designated.add((gu, dong))

    def is_designated(self, gu, dong):
        """이미 모아타운으로 선정된 지역인지 확인"""
        return (gu, dong) in self.designated

    def score_obsolescence(self, buildings_df):
        """노후도 점수 (0~100)

        - 30년 이상 건물 비율이 50% 이상이면 필수요건 충족
        - 비율이 높을수록 고점수
        """
        if buildings_df.empty:
            return 0

        current_year = datetime.now().year
        buildings_df = buildings_df.copy()
        buildings_df["건물나이"] = current_year - pd.to_numeric(
            buildings_df["사용승인연도"], errors="coerce"
        )

        total = len(buildings_df)
        old_count = len(buildings_df[buildings_df["건물나이"] >= 30])
        old_ratio = old_count / total if total > 0 else 0

        if old_ratio < 0.5:
            return old_ratio * 60  # 50% 미만이면 낮은 점수
        elif old_ratio < 0.67:
            return 60 + (old_ratio - 0.5) * 200  # 50~67%
        else:
            return min(100, 80 + (old_ratio - 0.67) * 60)  # 67%+

    def score_land_price(self, avg_land_price, seoul_avg=None):
        """사업성 점수 - 공시지가 (0~100)

        - 서울 평균 대비 낮을수록 유리
        - 보정계수 1.0~1.5 적용 기준
        """
        if seoul_avg is None:
            seoul_avg = 5000000  # 서울 평균 공시지가 (㎡당, 추정)

        if avg_land_price <= 0:
            return 50

        ratio = avg_land_price / seoul_avg
        if ratio <= 0.3:
            return 100
        elif ratio <= 0.5:
            return 80
        elif ratio <= 0.7:
            return 60
        elif ratio <= 1.0:
            return 40
        elif ratio <= 1.5:
            return 20
        else:
            return 5

    def score_far_gap(self, current_far, allowed_far):
        """용적률 갭 점수 (0~100)

        - 현재 용적률 vs 허용 용적률의 갭이 클수록 유리
        """
        if current_far <= 0 or allowed_far <= 0:
            return 50

        gap = allowed_far - current_far
        gap_ratio = gap / allowed_far

        if gap_ratio >= 0.6:
            return 100
        elif gap_ratio >= 0.4:
            return 80
        elif gap_ratio >= 0.2:
            return 60
        else:
            return 30

    def score_infrastructure(self, road_width_avg=None, parking_ratio=None, park_distance=None):
        """기반시설 부족도 점수 (0~100)

        - 도로 좁고, 주차장 부족하고, 공원 멀수록 높은 점수 (역설적)
        """
        score = 50  # 기본

        if road_width_avg is not None:
            if road_width_avg < 4:
                score += 20
            elif road_width_avg < 6:
                score += 10
            else:
                score -= 10

        if parking_ratio is not None:
            if parking_ratio < 0.5:
                score += 15
            elif parking_ratio < 1.0:
                score += 5
            else:
                score -= 10

        if park_distance is not None:
            if park_distance > 500:
                score += 15
            elif park_distance > 300:
                score += 5

        return max(0, min(100, score))

    def score_resident_consent(self, church_count=0, large_commercial=0,
                                new_building_ratio=0, owner_occupied_ratio=0.5):
        """주민동의 용이성 점수 (0~100)

        - 교회 많으면 크게 감점
        - 대형 상가 있으면 감점
        - 신축 비율 높으면 감점
        - 실거주율 높으면 가점
        """
        score = 70  # 기본

        # 교회 (가장 큰 저해요인)
        score -= church_count * 15

        # 대형 상가
        score -= large_commercial * 10

        # 신축 비율 (10년 미만)
        score -= new_building_ratio * 50

        # 실거주율
        score += (owner_occupied_ratio - 0.5) * 40

        return max(0, min(100, score))

    def score_adjacent_projects(self, nearby_redevelopment=0, nearby_reconstruction=0):
        """인접 정비사업 점수 (0~100)"""
        score = 30
        score += nearby_redevelopment * 20
        score += nearby_reconstruction * 15
        return min(100, score)

    def score_policy_momentum(self, year=None):
        """정책 모멘텀 점수 (0~100)

        - 2022~2024: 대량 선정기 (높은 점수)
        - 2025~: 속도 조절 가능성
        - 선거 주기 고려
        """
        if year is None:
            year = datetime.now().year

        # 서울시 모아타운 선정 추이
        if year <= 2024:
            return 90  # 적극 추진기
        elif year == 2025:
            return 70  # 지속 추진, 속도 조절 가능
        elif year == 2026:
            return 60  # 지방선거 해 → 정책 가속 가능
        else:
            return 40  # 불확실

    def calculate_total_score(self, scores):
        """종합 점수 계산 (가중 평균)

        Args:
            scores: dict with keys matching WEIGHTS

        Returns:
            float: 0~100 종합 점수
        """
        total = 0
        weight_sum = 0

        for factor, weight in self.WEIGHTS.items():
            if factor in scores:
                total += scores[factor] * weight
                weight_sum += weight

        if weight_sum == 0:
            return 0

        return round(total / weight_sum, 1)

    def score_area(self, area_data):
        """특정 지역(동) 종합 스코어링

        Args:
            area_data: dict with area information

        Returns:
            dict: 항목별 점수 + 종합 점수
        """
        scores = {}

        # 노후도
        if "buildings" in area_data:
            scores["노후도"] = self.score_obsolescence(area_data["buildings"])

        # 사업성 (공시지가)
        if "avg_land_price" in area_data:
            scores["사업성_공시지가"] = self.score_land_price(
                area_data["avg_land_price"],
                area_data.get("seoul_avg_land_price"),
            )

        # 용적률 갭
        if "current_far" in area_data and "allowed_far" in area_data:
            scores["용적률_갭"] = self.score_far_gap(
                area_data["current_far"],
                area_data["allowed_far"],
            )

        # 기반시설
        scores["기반시설_부족도"] = self.score_infrastructure(
            area_data.get("road_width_avg"),
            area_data.get("parking_ratio"),
            area_data.get("park_distance"),
        )

        # 주민동의
        scores["주민동의_용이성"] = self.score_resident_consent(
            area_data.get("church_count", 0),
            area_data.get("large_commercial", 0),
            area_data.get("new_building_ratio", 0),
            area_data.get("owner_occupied_ratio", 0.5),
        )

        # 면적 적합성
        if "block_area" in area_data:
            area_m2 = area_data["block_area"]
            if area_m2 <= 100000:
                scores["면적_적합성"] = 100
            elif area_m2 <= 150000:
                scores["면적_적합성"] = 50
            else:
                scores["면적_적합성"] = 10

        # 인접 정비사업
        scores["인접_정비사업"] = self.score_adjacent_projects(
            area_data.get("nearby_redevelopment", 0),
            area_data.get("nearby_reconstruction", 0),
        )

        # 정책 모멘텀
        scores["정책_모멘텀"] = self.score_policy_momentum()

        # 종합
        total = self.calculate_total_score(scores)

        return {
            "항목별_점수": scores,
            "종합_점수": total,
            "등급": self._get_grade(total),
        }

    def _get_grade(self, score):
        """점수 → 등급"""
        if score >= 80:
            return "A (매우 유력)"
        elif score >= 65:
            return "B (유력)"
        elif score >= 50:
            return "C (가능성 있음)"
        elif score >= 35:
            return "D (가능성 낮음)"
        else:
            return "F (가능성 희박)"


# ============================================================
# 4. 건축물대장 데이터 수집
# ============================================================
def fetch_building_data(sigungu_cd, bjdong_cd, api_key, num_of_rows=1000):
    """건축물대장 API에서 건물 데이터 수집

    Returns:
        DataFrame with columns: 건물명, 주용도, 사용승인일, 사용승인연도,
                                 대지면적, 연면적, 용적률, 건폐율, 구조
    """
    import requests

    url = (
        f"https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"
        f"?serviceKey={api_key}&sigunguCd={sigungu_cd}&bjdongCd={bjdong_cd}"
        f"&platGbCd=0&numOfRows={num_of_rows}&pageNo=1&type=json"
    )

    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return pd.DataFrame()

        data = r.json()
        items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])
        if isinstance(items, dict):
            items = [items]

        if not items:
            return pd.DataFrame()

        records = []
        for item in items:
            use_apr = str(item.get("useAprDay", ""))
            year = use_apr[:4] if len(use_apr) >= 4 else ""

            records.append({
                "건물명": item.get("bldNm", ""),
                "주용도": item.get("mainPurpsCdNm", ""),
                "사용승인일": use_apr,
                "사용승인연도": year,
                "대지면적": item.get("platArea", 0),
                "연면적": item.get("totArea", 0),
                "용적률": item.get("vlRat", 0),
                "건폐율": item.get("bcRat", 0),
                "구조": item.get("strctCdNm", ""),
                "지번": item.get("platPlc", ""),
            })

        return pd.DataFrame(records)

    except Exception as e:
        print(f"건축물대장 API 오류: {e}")
        return pd.DataFrame()


def filter_villa_buildings(buildings_df):
    """다세대/다가구/연립 건물만 필터링"""
    if buildings_df.empty:
        return buildings_df

    villa_keywords = ["다세대", "다가구", "연립", "주택"]
    mask = buildings_df["주용도"].str.contains("|".join(villa_keywords), na=False)
    return buildings_df[mask].copy()


# ============================================================
# 5. 메인 실행
# ============================================================
if __name__ == "__main__":
    scorer = MoatownScorer()

    # 테스트: 가상 지역 데이터로 스코어링
    print("=== 모아타운 스코어링 엔진 테스트 ===\n")

    test_areas = [
        {
            "name": "테스트A: 노후 빌라 밀집 (선정 유력)",
            "buildings": pd.DataFrame({
                "사용승인연도": ["1985", "1988", "1990", "1992", "1978",
                            "1986", "1991", "1989", "1993", "1987"]
            }),
            "avg_land_price": 2000000,
            "current_far": 120,
            "allowed_far": 250,
            "church_count": 0,
            "large_commercial": 0,
            "new_building_ratio": 0.05,
            "owner_occupied_ratio": 0.7,
            "block_area": 50000,
            "nearby_redevelopment": 1,
        },
        {
            "name": "테스트B: 신축 혼합 + 교회 (선정 어려움)",
            "buildings": pd.DataFrame({
                "사용승인연도": ["2015", "2018", "1990", "1992", "1978",
                            "2020", "1991", "2019", "1993", "1987"]
            }),
            "avg_land_price": 8000000,
            "current_far": 200,
            "allowed_far": 250,
            "church_count": 2,
            "large_commercial": 1,
            "new_building_ratio": 0.4,
            "owner_occupied_ratio": 0.3,
            "block_area": 80000,
        },
        {
            "name": "테스트C: 중간 (모니터링 대상)",
            "buildings": pd.DataFrame({
                "사용승인연도": ["1995", "1988", "1990", "1992", "1998",
                            "1986", "2005", "1989", "1993", "2010"]
            }),
            "avg_land_price": 4000000,
            "current_far": 150,
            "allowed_far": 250,
            "church_count": 1,
            "large_commercial": 0,
            "new_building_ratio": 0.15,
            "owner_occupied_ratio": 0.55,
            "block_area": 70000,
            "nearby_redevelopment": 1,
        },
    ]

    for area in test_areas:
        name = area.pop("name")
        result = scorer.score_area(area)
        print(f"📍 {name}")
        print(f"   종합 점수: {result['종합_점수']} / 100")
        print(f"   등급: {result['등급']}")
        for factor, score in result["항목별_점수"].items():
            print(f"   - {factor}: {score:.0f}")
        print()
