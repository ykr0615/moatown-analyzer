"""Claude AI 기반 부동산 투자 분석 리포트 생성"""
import anthropic
import pandas as pd
from config import ANTHROPIC_API_KEY


client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def generate_investment_report(
    market_summary: dict,
    gu_analysis: pd.DataFrame,
    jeonse_ratio: pd.DataFrame,
    candidates: pd.DataFrame,
    budget: int,
    strategy: str,
) -> str:
    """AI 투자 분석 리포트 생성"""

    prompt = f"""당신은 서울 부동산 투자 전문 분석가입니다.
아래 데이터를 기반으로 투자자에게 맞춤형 투자 분석 리포트를 작성해주세요.

## 투자자 정보
- 투자 가용 자산: {budget:,}만원 ({budget/10000:.1f}억원)
- 투자 전략: {"갭투자 (전세 끼고 매수)" if strategy == "gap" else "직접 매수"}

## 서울 아파트 시장 요약
- 최근 6개월 총 거래건수: {market_summary.get('총거래건수', 0):,}건
- 평균 매매가: {market_summary.get('평균매매가', 0):,}만원
- 중위 매매가: {market_summary.get('중위매매가', 0):,}만원
- 평균 평당가격: {market_summary.get('평균평당가격', 0):,}만원

## 구별 시세 현황 (상위 10개 구)
{gu_analysis.head(10).to_string() if not gu_analysis.empty else "데이터 없음"}

## 전세가율 현황 (상위 10개 구)
{jeonse_ratio.head(10).to_string() if not jeonse_ratio.empty else "데이터 없음"}

## 투자 가능 후보 매물 (상위 20개)
{candidates.head(20).to_string() if not candidates.empty else "예산 범위 내 매물 없음"}

## 요청사항
다음 항목을 포함한 상세 투자 리포트를 작성해주세요:

1. **시장 현황 분석**: 현재 서울 아파트 시장 상황 요약
2. **지역 추천**: 투자자의 예산과 전략에 맞는 지역 3곳 추천 (이유 포함)
3. **구체적 매물 추천**: 후보 매물 중 가장 유망한 3~5개 추천 (이유 포함)
4. **투자 전략 조언**: 현 시점에서의 투자 전략 제안
5. **리스크 분석**: 주의해야 할 리스크 요인
6. **정책 고려사항**: 현재 부동산 관련 정책 (대출규제, 세금 등) 고려사항

리포트는 한국어로 작성하고, 구체적인 숫자와 근거를 포함해주세요.
"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text


def ask_advisor(question: str, context: str = "") -> str:
    """자유 질문 응답"""
    prompt = f"""당신은 서울 부동산 투자 전문 분석가입니다.
사용자의 질문에 전문적이고 구체적으로 답변해주세요.

{f"참고 데이터:{chr(10)}{context}" if context else ""}

사용자 질문: {question}

한국어로 답변하고, 가능한 한 구체적인 숫자와 근거를 포함해주세요.
투자 관련 답변 시 반드시 리스크도 함께 언급해주세요.
"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text
