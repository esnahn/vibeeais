"""
hub.go.kr 원본 ZIP 다운로더

originals_list_collect.py 가 생성한 originals_list_YYYYMM.json 을 읽어
data/original/ 에 순차적으로 다운로드합니다.

사용법:
  - YEAR, MONTH를 수정한 뒤 실행하면 해당 연월의 원본 파일을 자동 다운로드합니다.
  - 필요한 경우 JSON 파일을 편집하여 특정 항목을 제외하거나 순서를 바꿀 수 있습니다.
"""

import json
import sys
from datetime import date
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# ── 다운로드할 목록(수집 대상 연월) 파일 지정 (매번 여기만 수정) ──────────────────
YEAR = "2025"
MONTH = "12"
# ──────────────────────────────────────────────────────────────────────────

BASE_URL = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"
DATA_DIR = Path("e:/vibeeais/data")
ORIGINAL_DIR = Path("e:/vibeeais/data/original")

_target = date(int(YEAR), int(MONTH), 1)
_end_month = int(MONTH) + 3
_end = date(int(YEAR) + (_end_month - 1) // 12, (_end_month - 1) % 12 + 1, 1)

# 날짜 범위: 3개월 후 1일까지 (업로드 지연 커버)
DATE_START = _target.strftime("%Y%m%d")
DATE_END = _end.strftime("%Y%m%d")

# 서비스명 검색어
SEARCH_KEYWORD = f"{YEAR}년 {int(MONTH):02d}월"

# 다운로드 목적 코드 (1=웹사이트개발, 4=공공업무, 5=연구 등)
DOWNLOAD_PURPOSE = "5"  # 연구(논문 등)


def download_items(items: list[dict]) -> list[Path]:
    """항목 목록을 순차적으로 다운로드하여 저장된 경로 목록을 반환."""
    ORIGINAL_DIR.mkdir(parents=True, exist_ok=True)
    saved_paths: list[Path] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(BASE_URL, wait_until="networkidle")

        # 검색 결과 페이지 열기 (fnDownloadPop JS 컨텍스트 확보)
        try:
            page.locator("#pageCountPerPage").select_option("40")
            page.wait_for_timeout(1000)
        except Exception:
            pass

        first_cat = items[0]["cat_val"]
        page.locator("#opnLgcptTaskSeCd").select_option(value=first_cat)
        page.locator("#startDay").fill(DATE_START)
        page.locator("#endDay").fill(DATE_END)
        page.locator("#srvcNm").fill(SEARCH_KEYWORD)
        page.locator("button.imp01").click()
        page.wait_for_timeout(3000)

        print(f"\n[다운로드] {len(items)}개 항목 순차 처리 시작\n")

        for idx, item in enumerate(items, 1):
            cat_val = item["cat_val"]
            task_code = item["task_code"]
            opn_code = item["opn_code"]
            service_name = item["service_name"]
            cat_name = item["cat_name"]

            print(f"  [{idx}/{len(items)}] {cat_name} / {service_name}")

            # 카테고리가 바뀌면 재검색
            current_cat = page.locator("#opnLgcptTaskSeCd").input_value()
            if current_cat != cat_val:
                page.locator("#opnLgcptTaskSeCd").select_option(value=cat_val)
                page.locator("button.imp01").click()
                page.wait_for_timeout(3000)

            try:
                page.evaluate(f"fnDownloadPop('{cat_val}','{task_code}','{opn_code}')")
                page.wait_for_timeout(700)

                # 사용 목적 라디오 선택
                radio = page.locator(
                    f"input[name='prpsCd'][value='{DOWNLOAD_PURPOSE}']"
                )
                if radio.count() == 0:
                    radio = page.locator("input[name='prpsCd']").first
                radio.click()
                page.wait_for_timeout(300)

                # 확인 버튼 클릭 → 다운로드 시작 (10분 타임아웃)
                with page.expect_download(timeout=600000) as dl_info:
                    page.locator("#fnInsertLog").click()

                dl = dl_info.value
                filename = dl.suggested_filename
                save_path = ORIGINAL_DIR / filename

                if save_path.exists():
                    print(f"    → 이미 존재, 건너뜀: {filename}")
                    dl.cancel()
                else:
                    dl.save_as(save_path)
                    size_mb = save_path.stat().st_size / 1024 / 1024
                    print(f"    → 저장 완료: {filename}  ({size_mb:.1f} MB)")
                    saved_paths.append(save_path)

                page.wait_for_timeout(5000)

            except Exception as e:
                print(f"    → 실패: {e}")
                # 팝업이 열려 있을 경우 닫기
                try:
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(300)
                except Exception:
                    pass

        browser.close()

    return saved_paths


def run():
    list_path = DATA_DIR / f"originals_list_{YEAR}{int(MONTH):02d}.json"

    if not list_path.exists():
        print(f"목록 파일이 없습니다: {list_path}")
        print("먼저 originals_list_collect.py 를 실행하세요.")
        return

    with open(list_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    print(f"목록 파일 로드: {list_path.name}  ({len(items)}개 항목)")

    saved = download_items(items)

    print("\n=== 최종 결과 ===")
    print(f"  신규 다운로드: {len(saved)}개")
    for path in saved:
        print(f"    - {path.name}")


if __name__ == "__main__":
    run()
