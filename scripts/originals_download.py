"""
hub.go.kr 원본 ZIP 다운로더

originals_list_collect.py 가 생성한 originals_list_YYYYMM.json 을 읽어
data/original/YYYYMM/ 에 순차적으로 다운로드합니다.

사용법:
  - YEAR, MONTH를 수정한 뒤 실행하면 해당 연월의 원본 파일을 자동 다운로드합니다.
  - 필요한 경우 JSON 파일을 편집하여 특정 항목을 제외하거나 순서를 바꿀 수 있습니다.
  - 실패 항목은 originals_list_YYYYMM_failed.json 에 저장됩니다. 재시도하려면
    RETRY_FAILED = True 로 설정 후 다시 실행하세요.
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
RETRY_FAILED = False  # True면 _failed.json을 읽어 실패 항목만 다시 다운로드합니다.
# ──────────────────────────────────────────────────────────────────────────

BASE_URL = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"
_BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = _BASE_DIR / "data"
ORIGINAL_DIR = DATA_DIR / "original" / f"{YEAR}{int(MONTH):02d}"

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

# 팝업 클릭 후 브라우저가 다운로드를 트리거할 때까지의 대기 시간
DOWNLOAD_TIMEOUT = 180_000  # 3분


def download_items(items: list[dict]) -> tuple[list[Path], list[dict]]:
    """항목 목록을 순차적으로 다운로드하여 (저장 경로 목록, 실패 항목 목록)을 반환."""
    ORIGINAL_DIR.mkdir(parents=True, exist_ok=True)
    saved_paths: list[Path] = []
    failed_items: list[dict] = []

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

                # 확인 버튼 클릭 → 다운로드 트리거 대기 (DOWNLOAD_TIMEOUT)
                with page.expect_download(timeout=DOWNLOAD_TIMEOUT) as dl_info:
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
                failed_items.append(item)
                # 팝업이 열려 있을 경우 닫기
                try:
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(300)
                except Exception:
                    pass

        browser.close()

    return saved_paths, failed_items


def run():
    base_name = f"originals_list_{YEAR}{int(MONTH):02d}"
    if RETRY_FAILED:
        list_path = DATA_DIR / "original" / f"{base_name}_failed.json"
    else:
        list_path = DATA_DIR / "original" / f"{base_name}.json"

    if not list_path.exists():
        print(f"목록 파일이 없습니다: {list_path}")
        if RETRY_FAILED:
            print("실패 항목이 없거나 아직 수집되지 않았습니다.")
        else:
            print("먼저 originals_list_collect.py 를 실행하세요.")
        return

    with open(list_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    print(f"목록 파일 로드: {list_path.name}  ({len(items)}개 항목)")

    saved, failed = download_items(items)

    print("\n=== 최종 결과 ===")
    print(f"  신규 다운로드: {len(saved)}개")
    for path in saved:
        print(f"    - {path.name}")

    if failed:
        failed_path = DATA_DIR / "original" / f"{base_name}_failed.json"
        with open(failed_path, "w", encoding="utf-8") as f:
            json.dump(failed, f, ensure_ascii=False, indent=2)
            f.write("\n")
        print(f"\n  실패: {len(failed)}개 → {failed_path.name} 저장됨")
        print("  재시도하려면 RETRY_FAILED = True 로 설정 후 재실행하세요.")
    elif RETRY_FAILED:
        # 실패 재시도에서 모두 성공하면 _failed 파일 삭제
        print("\n  모든 실패 항목 다운로드 성공!")
        if list_path.exists():
            list_path.unlink()

    print("\n다운로드한 파일을 검토한 뒤 build_catalog.py 를 실행하세요.")


if __name__ == "__main__":
    run()
