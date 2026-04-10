import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
DATA_GO_KR_API_KEY = os.getenv("DATA_GO_KR_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# 서울 지역코드 (법정동 코드 앞 5자리)
SEOUL_GU_CODES = {
    "강남구": "11680",
    "강동구": "11740",
    "강북구": "11305",
    "강서구": "11500",
    "관악구": "11620",
    "광진구": "11215",
    "구로구": "11530",
    "금천구": "11545",
    "노원구": "11350",
    "도봉구": "11320",
    "동대문구": "11230",
    "동작구": "11590",
    "마포구": "11440",
    "서대문구": "11410",
    "서초구": "11650",
    "성동구": "11200",
    "성북구": "11290",
    "송파구": "11710",
    "양천구": "11470",
    "영등포구": "11560",
    "용산구": "11170",
    "은평구": "11380",
    "종로구": "11110",
    "중구": "11140",
    "중랑구": "11260",
}

# 국토교통부 API 엔드포인트
API_ENDPOINTS = {
    "apt_trade": "http://openapi.molit.go.kr/OpenAPI_ToolInstall498/service/rest/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    "apt_trade_detail": "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPack498/service/rest/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
    "apt_rent": "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
}
