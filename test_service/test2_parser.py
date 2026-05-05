from __future__ import annotations

import json
import random
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


BASE_URL = "https://www.avito.ru"

START_URL = "https://www.avito.ru/sankt-peterburg/kvartiry/prodam-ASgBAgICAUSSA8YQ"

DATA_DIR = Path("data")

LINKS_CSV = DATA_DIR / "avito_links.csv"
DETAILS_CSV = DATA_DIR / "avito_details.csv"

LINKS_EXCEL = DATA_DIR / "avito_links.xlsm"
DETAILS_EXCEL = DATA_DIR / "avito_details.xlsm"

# Сколько страниц выдачи обойти для теста
MAX_LINK_PAGES = 2

# Сколько карточек подробно разобрать для теста
MAX_DETAIL_CARDS = 5

# Запускать ли второй этап после сбора ссылок
COLLECT_DETAILS_AFTER_LINKS = True

# Паузы
PAGE_DELAY_SECONDS = (20, 60)
CARD_DELAY_SECONDS = (20, 60)
INITIAL_WAIT_MS = (3000, 7000)

# Сохранять ли большой текст страницы карточки.
# Для отладки полезно, но CSV/Excel могут быстро разрастись.
SAVE_FULL_PAGE_TEXT = False

# Excel имеет ограничение примерно 32767 символов в ячейке
FULL_TEXT_LIMIT = 30000



LINK_COLUMNS = [
    "item_id",
    "title",
    "price",
    "source_page_number",
    "collected_at",
    "url",

]

DETAIL_COLUMNS = [
    "item_id",
    "title",
    "price",
    "address",
    "description",
    "seller_name",
    "params_json",
    "full_page_text",
    "collected_at",
    "url",

]

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_text(value: str | None) -> str | None:
    if not value:
        return None

    value = value.replace("\xa0", " ").replace("\u202f", " ")
    value = re.sub(r"\s+", " ", value)
    value = value.strip()

    return value or None


def normalize_multiline(value: str | None) -> str | None:
    if not value:
        return None

    value = value.replace("\xa0", " ").replace("\u202f", " ")
    lines = [line.strip() for line in value.splitlines()]
    lines = [line for line in lines if line]
    result = "\n".join(lines)

    return result or None


def extract_item_id(url: str) -> str | None:
    """
    У Avito ID часто находится в конце ссылки:
    ..._1234567890
    """
    match = re.search(r"_(\d+)(?:\?|$)", url)
    return match.group(1) if match else None


def sleep_random(bounds: tuple[int, int]) -> None:
    time.sleep(random.uniform(bounds[0], bounds[1]))


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)

    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)


def save_table(df: pd.DataFrame, csv_path: Path, excel_path: Path, sheet_name: str) -> None:
    """
    Сохраняет таблицу в CSV и Excel-дубликат.

    Важно:
    .xlsm нужен обычно для файлов с макросами.
    Здесь макросов нет, это просто Excel-дубликат данных.
    Если Excel будет ругаться на формат .xlsm, замени расширение на .xlsx.
    """
    ensure_data_dir()

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])


def upsert_rows(
    rows: list[dict],
    csv_path: Path,
    excel_path: Path,
    columns: list[str],
    key_column: str,
    sheet_name: str,
) -> pd.DataFrame:
    if not rows:
        return load_csv(csv_path, columns)

    old_df = load_csv(csv_path, columns)
    new_df = pd.DataFrame(rows)

    for column in columns:
        if column not in old_df.columns:
            old_df[column] = ""
        if column not in new_df.columns:
            new_df[column] = ""

    old_df = old_df[columns]
    new_df = new_df[columns]

    result_df = pd.concat([old_df, new_df], ignore_index=True)
    result_df = result_df.drop_duplicates(subset=[key_column], keep="last")
    result_df = result_df.sort_values(by=key_column).reset_index(drop=True)

    save_table(result_df, csv_path, excel_path, sheet_name)

    return result_df


def looks_like_captcha(html: str) -> bool:
    lowered = html.lower()

    markers = [
        "geetest_captcha"
    ]
    return any(marker in lowered for marker in markers)


def get_page_html_with_captcha_pause(page: Page) -> str | None:
    """
    Не автоматизирует прохождение капчи.
    Просто останавливает сбор и даёт тебе возможность вручную решить проверку в браузере.
    """
    html = page.content()

    if not looks_like_captcha(html):
        return html

    print("\nПоявилась проверка/капча.")
    print("Реши её вручную в открытом браузере.")
    input("После решения нажми Enter в терминале...")

    page.wait_for_timeout(3000)
    html = page.content()

    # if looks_like_captcha(html):
    #     print("Проверка всё ещё на странице. Останавливаю сбор, чтобы не усиливать блокировку.")
    #     return None

    return html

def parse_links(html: str, source_page_url: str, source_page_number: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    result = []

    cards = soup.find_all("div", {"data-marker": "item"})

    for card in cards:
        title_tag = card.find("a", {"data-marker": "item-title"})
        if not title_tag:
            continue

        href = title_tag.get("href")
        if not href:
            continue

        url = urljoin(BASE_URL, href)

        title = normalize_text(title_tag.get_text(" ", strip=True))

        price_tag = card.find("span", {"data-marker": "item-price-value"})
        price = normalize_text(price_tag.get_text(" ", strip=True)) if price_tag else None

        result.append(
            {
                "item_id": extract_item_id(url),
                "title": title,
                "price": price,
                "source_page_url": source_page_url,
                "source_page_number": str(source_page_number),
                "collected_at": now_iso(),
                "url": url,
            }
        )

    return result


def get_next_page_url(page: Page) -> str | None:
    selectors = [
        'a[aria-label*="Следующая"]',
        'a[data-marker="pagination-button/nextPage"]',
        'a:has-text("Следующая")',
    ]

    for selector in selectors:
        locator = page.locator(selector)

        if locator.count() == 0:
            continue

        href = locator.first.get_attribute("href")
        if not href:
            continue

        return urljoin(BASE_URL, href)

    return None


def collect_links() -> None:
    print("\n=== ЭТАП 1: сбор ссылок ===")

    ensure_data_dir()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        context = browser.new_context(
            locale="ru-RU",
            viewport={"width": 1366, "height": 768},
        )

        page = context.new_page()

        page.goto(START_URL, wait_until="domcontentloaded", timeout=60_000)

        seen_pages = set()
        parsed_pages = 0

        try:
            while parsed_pages < MAX_LINK_PAGES:
                current_url = page.url

                if current_url in seen_pages:
                    print("Эта страница уже была обработана. Останавливаю сбор ссылок.")
                    break

                seen_pages.add(current_url)
                parsed_pages += 1

                page.wait_for_timeout(random.randint(*INITIAL_WAIT_MS))

                html = get_page_html_with_captcha_pause(page)
                if html is None:
                    break

                rows = parse_links(
                    html=html,
                    source_page_url=current_url,
                    source_page_number=parsed_pages,
                )

                upsert_rows(
                    rows=rows,
                    csv_path=LINKS_CSV,
                    excel_path=LINKS_EXCEL,
                    columns=LINK_COLUMNS,
                    key_column="url",
                    sheet_name="links",
                )

                print(f"\nСтраница: {current_url}")
                print(f"Номер страницы в текущем запуске: {parsed_pages}/{MAX_LINK_PAGES}")
                print(f"Найдено ссылок на странице: {len(rows)}")
                print(f"Ссылки сохранены в: {LINKS_CSV}")
                print(f"Excel-дубликат: {LINKS_EXCEL}")

                if parsed_pages >= MAX_LINK_PAGES:
                    print("Достигнут лимит MAX_LINK_PAGES.")
                    break

                next_url = get_next_page_url(page)

                if not next_url:
                    print("Не нашёл следующую страницу.")
                    break

                print(f"Следующая страница: {next_url}")

                sleep_random(PAGE_DELAY_SECONDS)
                page.goto(next_url, wait_until="domcontentloaded", timeout=60_000)

        finally:
            browser.close()


# =========================
# ПАРСИНГ КАРТОЧКИ
# =========================

def get_first_text(soup: BeautifulSoup, selectors: list[str]) -> str | None:
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = normalize_text(node.get_text(" ", strip=True))
            if text:
                return text

    return None


def parse_params(soup: BeautifulSoup) -> dict[str, str]:
    params: dict[str, str] = {}

    candidates = []

    selectors = [
        '[data-marker*="item-view/item-params"] li',
        '[data-marker*="item-view/params"] li',
        '[class*="params"] li',
        "li",
    ]

    for selector in selectors:
        candidates.extend(soup.select(selector))

    for node in candidates:
        text = normalize_text(node.get_text(" ", strip=True))

        if not text:
            continue

        if ":" not in text:
            continue

        if len(text) > 250:
            continue

        key, value = text.split(":", 1)

        key = normalize_text(key)
        value = normalize_text(value)

        if not key or not value:
            continue

        if len(key) > 80:
            continue

        params[key] = value

    return params


def parse_detail(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    title = get_first_text(
        soup,
        [
            "h1",
            '[data-marker="item-view/title-info"]',
            '[data-marker="item-view/title"]',
        ],
    )

    price = get_first_text(
        soup,
        [
            '[data-marker="item-view/item-price"]',
            '[data-marker="item-price"]',
            '[itemprop="price"]',
        ],
    )

    address = get_first_text(
        soup,
        [
            '[data-marker="item-view/item-address"]',
            '[data-marker="delivery/location"]',
            '[itemprop="address"]',
        ],
    )

    description = get_first_text(
        soup,
        [
            '[data-marker="item-view/item-description"]',
            '[itemprop="description"]',
        ],
    )

    seller_name = get_first_text(
        soup,
        [
            '[data-marker="seller-info/name"]',
            '[data-marker="user-info/name"]',
            '[data-marker="seller-info/label"]',
        ],
    )

    params = parse_params(soup)

    full_page_text = None
    if SAVE_FULL_PAGE_TEXT:
        full_page_text = normalize_multiline(soup.get_text("\n", strip=True))
        if full_page_text:
            full_page_text = full_page_text[:FULL_TEXT_LIMIT]

    return {
        "url": url,
        "item_id": extract_item_id(url),
        "title": title,
        "price": price,
        "address": address,
        "description": description,
        "seller_name": seller_name,
        "params_json": json.dumps(params, ensure_ascii=False),
        "full_page_text": full_page_text,
        "collected_at": now_iso(),
    }


def get_links_for_details() -> list[str]:
    links_df = load_csv(LINKS_CSV, LINK_COLUMNS)

    if links_df.empty:
        return []

    details_df = load_csv(DETAILS_CSV, DETAIL_COLUMNS)

    already_done = set()
    if not details_df.empty and "url" in details_df.columns:
        already_done = set(details_df["url"].dropna().astype(str))

    urls = []

    for url in links_df["url"].dropna().astype(str):
        if not url:
            continue

        if url in already_done:
            continue

        urls.append(url)

    return urls[:MAX_DETAIL_CARDS]


def collect_details() -> None:
    print("\n=== ЭТАП 2: сбор подробной информации ===")

    urls = get_links_for_details()

    if not urls:
        print("Нет новых ссылок для подробного парсинга.")
        return

    print(f"Будет обработано карточек: {len(urls)}/{MAX_DETAIL_CARDS}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        context = browser.new_context(
            locale="ru-RU",
            viewport={"width": 1366, "height": 768},
        )

        page = context.new_page()

        try:
            for index, url in enumerate(urls, start=1):
                print(f"\nКарточка {index}/{len(urls)}")
                print(url)

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                    page.wait_for_timeout(random.randint(*INITIAL_WAIT_MS))

                    html = get_page_html_with_captcha_pause(page)
                    if html is None:
                        break

                    row = parse_detail(html, url)

                    upsert_rows(
                        rows=[row],
                        csv_path=DETAILS_CSV,
                        excel_path=DETAILS_EXCEL,
                        columns=DETAIL_COLUMNS,
                        key_column="url",
                        sheet_name="details",
                    )

                    print(f"Сохранено: {DETAILS_CSV}")
                    print(f"Excel-дубликат: {DETAILS_EXCEL}")

                    sleep_random(CARD_DELAY_SECONDS)

                except PlaywrightTimeoutError:
                    print("Timeout при загрузке карточки. Пропускаю.")
                    continue

                except Exception as exc:
                    print(f"Ошибка при обработке карточки: {exc!r}")
                    continue

        finally:
            browser.close()

def main() -> None:
    collect_links()

    if COLLECT_DETAILS_AFTER_LINKS:
        collect_details()


if __name__ == "__main__":
    main()