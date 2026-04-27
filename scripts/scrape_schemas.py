"""
hub.go.kr 스키마 스크레이퍼

사용법:
  YEAR, MONTH를 수정한 뒤 실행하면 해당 연월의 스키마를 자동 수집합니다.
  (해당 데이터는 보통 다음 달 초에 업로드되므로 DATE_END에 여유를 두었습니다.)
"""

import re
import sys
from datetime import date
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# ── 수집 대상 연월 (매번 여기만 수정) ──────────────────────────────────────
YEAR = "2025"
MONTH = "12"
# ──────────────────────────────────────────────────────────────────────────

_target = date(int(YEAR), int(MONTH), 1)
_end_month = int(MONTH) + 3
_end = date(int(YEAR) + (_end_month - 1) // 12, (_end_month - 1) % 12 + 1, 1)

BASE_URL = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"
SCHEMA_DIR = Path("e:/vibeeais/data/schema")

# 카테고리 코드 → 이름
CAT_MAP = {
    "01": "건축인허가",
    "02": "주택인허가",
    "03": "건축물대장",
}

# 날짜 범위: 3개월 후 1일까지 (업로드 지연 커버)
DATE_START = _target.strftime("%Y%m%d")
DATE_END = _end.strftime("%Y%m%d")

# 서비스명 검색어
SEARCH_KEYWORD = f"{YEAR}년 {int(MONTH)}월"


def clean_dataset_name(svc_name: str) -> str:
    """서비스명에서 ' (YYYY년 MM월)' 같은 날짜 접미사 제거."""
    name = re.sub(r"\s*\(\d{4}년\s*\d{1,2}월\)$", "", svc_name)
    name = re.sub(r"\s*\(\d{4}\.\d{1,2}\)$", "", name)
    name = re.sub(r"\s*\(\d{4}\)$", "", name)
    return name.strip()


def scrape_popup(page, btn_index: int) -> tuple[str, str, list[str]]:
    """팝업을 열고 서비스명, 제공기관, 스키마 행을 반환."""
    page.locator("[onclick*='fnLgcptPop']").nth(btn_index).click()
    page.wait_for_timeout(1200)

    soup = BeautifulSoup(page.content(), "html.parser")

    service_name = ""
    provider = ""
    divs = soup.find_all("div")
    for idx, div in enumerate(divs):
        text = div.text.strip()
        if text == "서비스명" and idx + 1 < len(divs):
            service_name = divs[idx + 1].text.strip()
        elif text == "제공기관/제공부서" and idx + 1 < len(divs):
            provider = divs[idx + 1].text.strip()

    schema_rows = []
    for table in soup.find_all("table"):
        ths = [th.text.strip() for th in table.find_all("th")]
        if "컬럼한글명" in ths:
            for tr in table.find_all("tr"):
                cols = [
                    td.text.strip().replace("\n", " ")
                    for td in tr.find_all(["th", "td"])
                ]
                if cols:
                    schema_rows.append("\t".join(cols))
            break

    # 팝업 닫기
    try:
        close_btns = page.locator(
            "button:has-text('닫기'), a:has-text('닫기'), .btn-close, .pop-close"
        ).all()
        for cb in close_btns:
            if cb.is_visible():
                cb.click()
                break
    except Exception:
        pass
    page.keyboard.press("Escape")
    page.wait_for_timeout(600)

    return service_name, provider, schema_rows


def save_schema(cat_name: str, svc_name: str, provider: str, schema_rows: list[str]):
    dataset_name = clean_dataset_name(svc_name)
    filename = SCHEMA_DIR / f"schema_{cat_name}_{dataset_name}.txt"

    content = [
        "서비스명",
        svc_name,
        "제공기관/제공부서",
        provider,
        "상세설명 표",
    ] + schema_rows

    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(content) + "\n")

    print(f"  → 저장: {filename.name}")
    return dataset_name


def search_and_collect(page, cat_val: str, cat_name: str):
    """카테고리 선택 → 검색 → 전 페이지 팝업 수집."""
    print(f"\n{'=' * 50}")
    print(f"[{cat_name}] 검색 시작")
    print(f"  날짜 범위: {DATE_START} ~ {DATE_END}")
    print(f"  검색어: {SEARCH_KEYWORD}")

    page.locator("#opnLgcptTaskSeCd").select_option(value=cat_val)
    page.wait_for_timeout(400)
    page.locator("#startDay").fill(DATE_START)
    page.locator("#endDay").fill(DATE_END)
    page.locator("#srvcNm").fill(SEARCH_KEYWORD)
    page.wait_for_timeout(300)

    page.locator("button.imp01").click()
    page.wait_for_timeout(3000)

    saved = []
    page_num = 1

    while True:
        btns = page.locator("[onclick*='fnLgcptPop']").all()
        print(f"\n  [페이지 {page_num}] 설명 버튼 {len(btns)}개")

        if not btns:
            print("  결과 없음. 다음 카테고리로 이동.")
            break

        for i in range(len(btns)):
            svc_name, provider, schema_rows = scrape_popup(page, i)

            if not svc_name or not schema_rows:
                print(f"  [{i}] 스키마 없음, 건너뜀 (service_name='{svc_name}')")
                continue

            dataset_name = save_schema(cat_name, svc_name, provider, schema_rows)
            saved.append(dataset_name)

        # 다음 페이지 확인
        next_btn = page.locator("a.next, a[class*='next'], a:has-text('다음으로 이동')")
        if next_btn.count() > 0 and next_btn.first.is_visible():
            next_btn.first.click()
            page.wait_for_timeout(2500)
            page_num += 1
        else:
            break

    print(f"\n[{cat_name}] 완료: {len(saved)}개 저장 → {saved}")
    return saved


def run():
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(BASE_URL, wait_until="networkidle")

        # 페이지당 40개
        try:
            page.locator("#pageCountPerPage").select_option("40")
            page.wait_for_timeout(1000)
        except Exception:
            pass

        all_saved = {}
        for cat_val, cat_name in CAT_MAP.items():
            saved = search_and_collect(page, cat_val, cat_name)
            all_saved[cat_name] = saved

        browser.close()

    print("\n=== 최종 결과 ===")
    for cat, items in all_saved.items():
        print(f"  {cat}: {len(items)}개")


if __name__ == "__main__":
    run()
