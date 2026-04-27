"""
hub.go.kr 다운로드 항목 수집기

사이트를 탐색하여 다운로드 가능한 원본 ZIP 파일 목록을 수집하고
data/originals_list_YYYYMM.json 에 저장합니다.

저장된 파일을 검토·편집한 뒤 originals_download.py 를 실행하세요.
"""

import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# ── 수집 대상 연월 (매번 여기만 수정) ─────────────────────────────────────────
YEAR = "2025"
MONTH = "12"
# ──────────────────────────────────────────────────────────────────────────

_target = date(int(YEAR), int(MONTH), 1)
_end_month = int(MONTH) + 3
_end = date(int(YEAR) + (_end_month - 1) // 12, (_end_month - 1) % 12 + 1, 1)

BASE_URL = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"
DATA_DIR = Path("e:/vibeeais/data")

# 카테고리 매핑 (필요할 때만 수정)
CAT_MAP = {
    "01": "건축인허가",
    "02": "주택인허가",
    "03": "건축물대장",
}

DATE_START = _target.strftime("%Y%m%d")
DATE_END = _end.strftime("%Y%m%d")
SEARCH_KEYWORD = f"{YEAR}년 {int(MONTH):02d}월"

_POP_PATTERN = re.compile(r"fnDownloadPop\('([^']+)','([^']+)','([^']+)'\)")


@dataclass
class DownloadItem:
    cat_val: str  # 카테고리 코드 (e.g. "01")
    cat_name: str  # 카테고리 이름 (e.g. "건축인허가")
    task_code: str  # 업무 코드    (e.g. "0117")
    opn_code: str  # 파일 식별 코드 (e.g. "OPN202601201217465990")
    service_name: str  # 서비스명     (e.g. "주택유형 (2025년 12월)")


def _collect_page_items(page: Page, cat_val: str, cat_name: str) -> list[DownloadItem]:
    """현재 검색 결과 페이지에서 다운로드 항목 메타데이터를 수집."""
    items: list[DownloadItem] = []

    dl_btns = page.locator("[onclick*='fnDownloadPop']").all()
    desc_btns = page.locator("[onclick*='fnLgcptPop']").all()

    for i, btn in enumerate(dl_btns):
        onclick = btn.get_attribute("onclick") or ""
        m = _POP_PATTERN.search(onclick)
        if not m:
            continue

        service_name = ""
        if i < len(desc_btns):
            desc_onclick = desc_btns[i].get_attribute("onclick") or ""
            sm = re.search(r"fnLgcptPop\('[^']+','[^']+','([^']+)'", desc_onclick)
            if sm:
                service_name = sm.group(1)

        items.append(
            DownloadItem(
                cat_val=m.group(1),
                cat_name=cat_name,
                task_code=m.group(2),
                opn_code=m.group(3),
                service_name=service_name,
            )
        )

    return items


def collect_download_list() -> list[DownloadItem]:
    """사이트를 탐색하여 다운로드 가능한 항목 목록을 반환."""
    all_items: list[DownloadItem] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(BASE_URL, wait_until="networkidle")

        try:
            page.locator("#pageCountPerPage").select_option("40")
            page.wait_for_timeout(1000)
        except Exception:
            pass

        for cat_val, cat_name in CAT_MAP.items():
            print(
                f"\n[수집] {cat_name}  ({DATE_START} ~ {DATE_END}, 검색어: '{SEARCH_KEYWORD}')"
            )

            page.locator("#opnLgcptTaskSeCd").select_option(value=cat_val)
            page.wait_for_timeout(400)
            page.locator("#startDay").fill(DATE_START)
            page.locator("#endDay").fill(DATE_END)
            page.locator("#srvcNm").fill(SEARCH_KEYWORD)
            page.locator("button.imp01").click()
            page.wait_for_timeout(3000)

            page_num = 1
            while True:
                items = _collect_page_items(page, cat_val, cat_name)
                print(f"  페이지 {page_num}: {len(items)}개")
                for item in items:
                    print(
                        f"    - [{item.cat_name}] {item.service_name}  ({item.opn_code})"
                    )
                all_items.extend(items)

                next_btn = page.locator("a:has-text('다음으로 이동')")
                if next_btn.count() > 0 and next_btn.first.is_visible():
                    next_btn.first.click()
                    page.wait_for_timeout(2500)
                    page_num += 1
                else:
                    break

        browser.close()

    return all_items


def run():
    items = collect_download_list()

    if not items:
        print("수집된 항목이 없습니다.")
        return

    out_path = DATA_DIR / f"originals_list_{YEAR}{int(MONTH):02d}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump([asdict(i) for i in items], f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"\n총 {len(items)}개 항목을 저장했습니다: {out_path}")
    print("파일을 검토한 뒤 originals_download.py 를 실행하세요.")


if __name__ == "__main__":
    run()
