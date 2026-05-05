from __future__ import annotations

import json
import random
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup, Tag
from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


# ============================================================
# ГЛОБАЛЬНЫЕ НАСТРОЙКИ
# ============================================================

BASE_URL = "https://www.avito.ru"
START_URL = "https://www.avito.ru/sankt-peterburg/kvartiry/prodam-ASgBAgICAUSSA8YQ"

DATA_DIR = Path("data")
DEBUG_DIR = DATA_DIR / "debug"
BROWSER_PROFILE_DIR = DATA_DIR / "browser_profile"

LINKS_CSV = DATA_DIR / "avito_links.csv"
DETAILS_CSV = DATA_DIR / "avito_details.csv"

# По просьбе пользователя дублируем в xlsm.
# Если Excel будет ругаться на формат, поменяй расширение на .xlsx.
LINKS_EXCEL = DATA_DIR / "avito_links.xlsm"
DETAILS_EXCEL = DATA_DIR / "avito_details.xlsm"

# Режим запуска:
#   "links"   — собрать только ссылки
#   "details" — собрать только подробные карточки из LINKS_CSV
#   "all"     — сначала ссылки, потом детали
RUN_MODE = "all"

# Сколько страниц выдачи обойти для теста.
MAX_LINK_PAGES = 1

# Сколько карточек подробно разобрать для теста.
MAX_DETAIL_CARDS = 2

# Использовать постоянный профиль браузера.
# Это НЕ обход капчи. Это просто сохранение cookies/localStorage после обычной ручной работы в браузере.
USE_PERSISTENT_PROFILE = True

# Пробовать запускать установленный в системе Chrome/Chromium.
# На Fedora это иногда удобнее, но если не получится — будет fallback на bundled Chromium от Playwright.
USE_SYSTEM_BROWSER_IF_FOUND = True

HEADLESS = False
VIEWPORT = {"width": 1366, "height": 768}
LOCALE = "ru-RU"

# Паузы. Для теста можно уменьшить, для стабильной работы лучше не делать слишком маленькими.
INITIAL_WAIT_MS = (5_000, 10_000)
PAGE_DELAY_SECONDS = (60, 120)
CARD_DELAY_SECONDS = (90, 180)

# Скролл выдачи перед парсингом ссылок.
SCROLL_LISTING_PAGE = True
LISTING_SCROLL_STEPS = 4
LISTING_SCROLL_DELAY_MS = (900, 1800)

# Скролл карточки перед парсингом деталей.
SCROLL_DETAIL_PAGE = True
DETAIL_SCROLL_STEPS = 3
DETAIL_SCROLL_DELAY_MS = (900, 1800)

# Поведение при капче/проверке.
MAX_CAPTCHA_ATTEMPTS = 2
PAGE_READY_TIMEOUT_SECONDS = 30

# Сохранять полный текст карточки.
# Полезно для отладки, но CSV/Excel быстро разрастаются.
SAVE_FULL_PAGE_TEXT = False

# Excel имеет ограничение около 32767 символов в ячейке.
FULL_TEXT_LIMIT = 30_000
EXCEL_CELL_LIMIT = 32_000

# Колонки, которые надо удалить из старых версий CSV.
DROP_COLUMNS = {"source_page_url"}


# ============================================================
# КОЛОНКИ ВЫХОДНЫХ ФАЙЛОВ
# ============================================================

LINK_COLUMNS = [
    "item_id",
    "title",
    "object_type",
    "space",
    "price",
    "area_price",
    "address",
    "metro",
    "time_to_metro",
    "published_raw",
    "published_date",
    "source_page_number",
    "collected_at",
    "url",
]

DETAIL_BASE_COLUMNS = [
    "item_id",
    "page_status",
    "title",
    "object_type",
    "space",
    "price",
    "address",
    "metro",
    "seller_name",
    "seller_type",
    "description",
    "canonical_url",
    "image_count",
    "image_urls_json",
    "params_json",
    "json_ld_json",
    "full_page_text",
    "collected_at",
    "url",
]


# ============================================================
# ОБЩИЕ УТИЛИТЫ
# ============================================================

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None

    value = str(value)
    value = value.replace("\xa0", " ").replace("\u202f", " ")
    value = value.replace("₽", "₽")
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def normalize_multiline(value: Any) -> str | None:
    if value is None:
        return None

    value = str(value).replace("\xa0", " ").replace("\u202f", " ")
    lines = [line.strip() for line in value.splitlines()]
    lines = [line for line in lines if line]
    result = "\n".join(lines).strip()
    return result or None


def clean_price(value: str | None) -> str | None:
    value = normalize_text(value)
    if not value:
        return None

    value = value.replace("от ", "")
    value = normalize_text(value)
    return value


def extract_item_id(url: str) -> str | None:
    match = re.search(r"_(\d+)(?:\?|$)", url)
    return match.group(1) if match else None


def parse_title_parts(title: str | None) -> tuple[str | None, str | None]:
    """
    Пример title из выдачи: "2-к. квартира, 54 м², 7/12 эт."
    object_type -> "2-к. квартира"
    space -> "54"
    """
    title = normalize_text(title)
    if not title:
        return None, None

    parts = [normalize_text(part) for part in title.split(",")]
    parts = [part for part in parts if part]

    object_type = parts[0] if parts else None
    space = None

    if len(parts) > 1 and parts[1]:
        match = re.search(r"(\d+(?:[,.]\d+)?)", parts[1])
        if match:
            space = match.group(1).replace(",", ".")

    return object_type, space


def normalize_date_text(value: str | None) -> str | None:
    """
    Упрощённая нормализация дат из выдачи.
    Возвращает дату в формате DD.MM.YYYY, если удалось распознать.
    """
    value = normalize_text(value)
    if not value:
        return None

    raw = value.lower()
    raw = raw.replace("–", " ").replace("—", " ")
    raw = raw.replace("в ", " ")
    raw = normalize_text(raw) or ""

    today = datetime.now()

    if "сегодня" in raw or "сейчас" in raw:
        return today.strftime("%d.%m.%Y")

    if "вчера" in raw:
        return (today - timedelta(days=1)).strftime("%d.%m.%Y")

    month_map = {
        "января": "01",
        "февраля": "02",
        "марта": "03",
        "апреля": "04",
        "мая": "05",
        "июня": "06",
        "июля": "07",
        "августа": "08",
        "сентября": "09",
        "октября": "10",
        "ноября": "11",
        "декабря": "12",
    }

    for month_name, month_number in month_map.items():
        if month_name in raw:
            day_match = re.search(r"\b(\d{1,2})\b", raw)
            if not day_match:
                return value

            day = int(day_match.group(1))
            return f"{day:02d}.{month_number}.{today.year}"

    relative_match = re.search(
        r"(\d+)\s+(секунд|минут|час|дн|день|дня|недел|месяц|год|лет)",
        raw,
    )

    if relative_match:
        amount = int(relative_match.group(1))
        unit = relative_match.group(2)

        if unit.startswith("секунд"):
            dt = today - timedelta(seconds=amount)
        elif unit.startswith("минут"):
            dt = today - timedelta(minutes=amount)
        elif unit.startswith("час"):
            dt = today - timedelta(hours=amount)
        elif unit.startswith("дн") or unit.startswith("день") or unit.startswith("дня"):
            dt = today - timedelta(days=amount)
        elif unit.startswith("недел"):
            dt = today - timedelta(days=amount * 7)
        elif unit.startswith("месяц"):
            dt = today - timedelta(days=amount * 30)
        else:
            dt = today - timedelta(days=amount * 365)

        return dt.strftime("%d.%m.%Y")

    return value


def sleep_random(bounds: tuple[int, int]) -> None:
    time.sleep(random.uniform(bounds[0], bounds[1]))


def load_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)

    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)

    for column in DROP_COLUMNS:
        if column in df.columns:
            df = df.drop(columns=[column])

    return df


def reorder_columns(df: pd.DataFrame, preferred_columns: list[str]) -> pd.DataFrame:
    preferred_without_url = [col for col in preferred_columns if col != "url" and col not in DROP_COLUMNS]

    ordered: list[str] = []

    for col in preferred_without_url:
        if col in df.columns and col not in ordered:
            ordered.append(col)

    extra_columns = [
        col
        for col in df.columns
        if col not in ordered and col != "url" and col not in DROP_COLUMNS
    ]

    # param_* удобно держать рядом, но после базовых колонок.
    extra_columns = sorted(extra_columns, key=lambda c: (not c.startswith("param_"), c))
    ordered.extend(extra_columns)

    if "url" in df.columns:
        ordered.append("url")

    return df[ordered]


def sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    excel_df = df.copy()

    for column in excel_df.columns:
        excel_df[column] = excel_df[column].map(
            lambda value: value[:EXCEL_CELL_LIMIT] if isinstance(value, str) else value
        )

    return excel_df


def save_table(df: pd.DataFrame, csv_path: Path, excel_path: Path, sheet_name: str) -> None:
    ensure_dirs()

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    excel_df = sanitize_for_excel(df)
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        excel_df.to_excel(writer, index=False, sheet_name=sheet_name[:31])


def upsert_rows(
    rows: list[dict[str, Any]],
    csv_path: Path,
    excel_path: Path,
    columns: list[str],
    key_column: str,
    sheet_name: str,
) -> pd.DataFrame:
    old_df = load_csv(csv_path, columns)

    if not rows:
        old_df = reorder_columns(old_df, columns)
        save_table(old_df, csv_path, excel_path, sheet_name)
        return old_df

    new_df = pd.DataFrame(rows)

    for column in DROP_COLUMNS:
        if column in new_df.columns:
            new_df = new_df.drop(columns=[column])

    all_columns = list(
        dict.fromkeys(
            [
                *columns,
                *old_df.columns.tolist(),
                *new_df.columns.tolist(),
            ]
        )
    )
    all_columns = [col for col in all_columns if col not in DROP_COLUMNS]

    for column in all_columns:
        if column not in old_df.columns:
            old_df[column] = ""
        if column not in new_df.columns:
            new_df[column] = ""

    old_df = old_df[all_columns]
    new_df = new_df[all_columns]

    result_df = pd.concat([old_df, new_df], ignore_index=True)
    result_df = result_df.drop_duplicates(subset=[key_column], keep="last")
    result_df = reorder_columns(result_df, columns)
    result_df = result_df.reset_index(drop=True)

    save_table(result_df, csv_path, excel_path, sheet_name)
    return result_df


# ============================================================
# БРАУЗЕР
# ============================================================

def find_system_browser() -> str | None:
    candidates = [
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
    ]

    for binary in candidates:
        path = shutil.which(binary)
        if path:
            return path

    return None


def create_browser_context(p) -> BrowserContext:
    ensure_dirs()

    executable_path = find_system_browser() if USE_SYSTEM_BROWSER_IF_FOUND else None

    common_kwargs: dict[str, Any] = {
        "headless": HEADLESS,
        "locale": LOCALE,
        "viewport": VIEWPORT,
    }

    if executable_path:
        common_kwargs["executable_path"] = executable_path
        print(f"Использую системный браузер: {executable_path}")

    if USE_PERSISTENT_PROFILE:
        try:
            return p.chromium.launch_persistent_context(
                user_data_dir=str(BROWSER_PROFILE_DIR),
                **common_kwargs,
            )
        except Exception as exc:
            if executable_path:
                print("Не удалось запустить системный браузер через Playwright.")
                print(f"Ошибка: {exc!r}")
                print("Пробую bundled Chromium от Playwright.")
                common_kwargs.pop("executable_path", None)
                return p.chromium.launch_persistent_context(
                    user_data_dir=str(BROWSER_PROFILE_DIR),
                    **common_kwargs,
                )
            raise

    browser = p.chromium.launch(**common_kwargs)
    return browser.new_context(locale=LOCALE, viewport=VIEWPORT)


def get_working_page(context: BrowserContext) -> Page:
    if context.pages:
        return context.pages[0]
    return context.new_page()


def safe_goto(page: Page, url: str) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        return True
    except PlaywrightTimeoutError:
        print(f"Timeout при загрузке: {url}")
        return False


def scroll_page(page: Page, steps: int, delay_ms: tuple[int, int]) -> None:
    for _ in range(steps):
        try:
            page.mouse.wheel(0, random.randint(500, 1200))
            page.wait_for_timeout(random.randint(*delay_ms))
        except Exception:
            break


# ============================================================
# КАПЧА / ПРОВЕРКА СТРАНИЦЫ
# ============================================================

def has_locator(page: Page, selector: str) -> bool:
    try:
        return page.locator(selector).count() > 0
    except Exception:
        return False


def visible_locator(page: Page, selector: str) -> bool:
    try:
        locator = page.locator(selector)
        return locator.count() > 0 and locator.first.is_visible(timeout=1000)
    except Exception:
        return False


def get_body_text(page: Page) -> str:
    try:
        return page.locator("body").inner_text(timeout=3000).lower()
    except Exception:
        return ""


def has_visible_captcha(page: Page) -> bool:
    visible_selectors = [
        'iframe[src*="captcha"]',
        'iframe[src*="geetest"]',
        '[id*="captcha"]',
        '[class*="captcha"]',
        '[class*="geetest"]',
    ]

    return any(visible_locator(page, selector) for selector in visible_selectors)


def classify_page(page: Page, expected: str) -> str:
    """
    expected:
      - "listing" для выдачи
      - "detail" для карточки

    Принцип:
      1. Видимая капча считается капчей.
      2. Если нормальный контент найден и видимой капчи нет — страница ok.
      3. Слово geetest/captcha в HTML само по себе больше не считается причиной остановки.
    """
    visible_captcha = has_visible_captcha(page)

    if expected == "listing":
        normal_content = has_locator(page, 'div[data-marker="item"]') or has_locator(
            page, 'a[data-marker="item-title"]'
        )
    elif expected == "detail":
        normal_content = (
            has_locator(page, "h1")
            or has_locator(page, '[data-marker="item-view/item-price"]')
            or has_locator(page, '[data-marker="item-view/item-description"]')
            or has_locator(page, '[itemprop="description"]')
        )
    else:
        normal_content = False

    if visible_captcha:
        return "captcha"

    if normal_content:
        return "ok"

    body_text = get_body_text(page)

    removed_markers = [
        "объявление снято",
        "объявление удалено",
        "объявление не найдено",
        "такой страницы нет",
        "страница не найдена",
    ]

    if any(marker in body_text for marker in removed_markers):
        return "ok"

    captcha_text_markers = [
        "captcha",
        "капча",
        "докажите",
        "вы не робот",
        "подтвердите, что вы не робот",
        "проверка безопасности",
    ]

    if any(marker in body_text for marker in captcha_text_markers):
        return "captcha"

    if "captcha" in page.url.lower():
        return "captcha"

    return "unknown"


def save_debug_page(page: Page, reason: str) -> None:
    ensure_dirs()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = DEBUG_DIR / f"{timestamp}_{reason}.html"
    screenshot_path = DEBUG_DIR / f"{timestamp}_{reason}.png"

    try:
        html_path.write_text(page.content(), encoding="utf-8")
        page.screenshot(path=str(screenshot_path), full_page=True)
        print(f"Debug HTML сохранён: {html_path}")
        print(f"Debug screenshot сохранён: {screenshot_path}")
    except Exception as exc:
        print(f"Не удалось сохранить debug-файлы: {exc!r}")


def get_page_html_with_manual_check(page: Page, expected: str) -> str | None:
    """
    Функция НЕ проходит капчу автоматически и НЕ обходит защиту.
    Она только даёт вручную решить проверку в открытом браузере и продолжает,
    если после этого на странице появился ожидаемый контент.
    """
    for attempt in range(MAX_CAPTCHA_ATTEMPTS + 1):
        deadline = time.time() + PAGE_READY_TIMEOUT_SECONDS

        while time.time() < deadline:
            state = classify_page(page, expected=expected)

            if state == "ok":
                return page.content()

            if state == "captcha":
                break

            page.wait_for_timeout(1000)

        if attempt < MAX_CAPTCHA_ATTEMPTS:
            print("\nПоявилась проверка или страница не загрузила ожидаемый контент.")
            print("Реши проверку вручную в открытом браузере.")
            input("После решения нажми Enter в терминале...")

            try:
                page.wait_for_load_state("domcontentloaded", timeout=30_000)
            except Exception:
                pass

            page.wait_for_timeout(5000)
            continue

        print("Страница так и не стала пригодной для парсинга. Текущий этап остановлен.")
        save_debug_page(page, reason=f"{expected}_not_ready")
        return None

    return None


# ============================================================
# ПАРСИНГ ВЫДАЧИ
# ============================================================

def get_tag_text(node: Tag | None) -> str | None:
    if not node:
        return None
    return normalize_text(node.get_text(" ", strip=True))


def parse_listing_card(card: Tag, source_page_number: int) -> dict[str, Any] | None:
    title_tag = card.find("a", {"data-marker": "item-title"})
    if not title_tag:
        return None

    href = title_tag.get("href")
    if not href:
        return None

    url = urljoin(BASE_URL, str(href))
    title = get_tag_text(title_tag)
    object_type, space = parse_title_parts(title)

    price_tag = card.find("span", {"data-marker": "item-price-value"})
    price = clean_price(get_tag_text(price_tag)) if price_tag else None

    area_price = None
    for p in card.find_all("p"):
        text = get_tag_text(p)
        if text and ("за м²" in text or "за м2" in text):
            area_price = text.replace("за м²", "").replace("за м2", "").strip()
            area_price = normalize_text(area_price)
            break

    street = card.find("a", {"data-marker": "street_link"})
    house = card.find("a", {"data-marker": "house_link"})
    address = ", ".join(part for part in [get_tag_text(street), get_tag_text(house)] if part)
    address = normalize_text(address)

    metro_link = card.find("a", {"data-marker": "metro_link"})
    metro = get_tag_text(metro_link)

    time_to_metro = None
    if metro_link:
        next_span = metro_link.find_next_sibling("span")
        if next_span:
            time_to_metro = get_tag_text(next_span)
            if time_to_metro:
                time_to_metro = time_to_metro.strip(", ")

    published_raw = None
    date_tag = card.find("div", {"data-marker": "item-date/wrapper"})
    if date_tag:
        published_raw = get_tag_text(date_tag)

    return {
        "item_id": extract_item_id(url),
        "title": title,
        "object_type": object_type,
        "space": space,
        "price": price,
        "area_price": area_price,
        "address": address,
        "metro": metro,
        "time_to_metro": time_to_metro,
        "published_raw": published_raw,
        "published_date": normalize_date_text(published_raw),
        "source_page_number": str(source_page_number),
        "collected_at": now_iso(),
        "url": url,
    }


def parse_links(html: str, source_page_number: int) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    result: list[dict[str, Any]] = []

    cards = soup.find_all("div", {"data-marker": "item"})

    for card in cards:
        if not isinstance(card, Tag):
            continue

        row = parse_listing_card(card, source_page_number=source_page_number)
        if row:
            result.append(row)

    return result


def get_next_page_url(page: Page) -> str | None:
    selectors = [
        'a[aria-label*="Следующая"]',
        'a[data-marker="pagination-button/nextPage"]',
        'a:has-text("Следующая")',
    ]

    for selector in selectors:
        try:
            locator = page.locator(selector)
            if locator.count() == 0:
                continue

            href = locator.first.get_attribute("href")
            if href:
                return urljoin(BASE_URL, href)
        except Exception:
            continue

    return None


def collect_links() -> None:
    print("\n=== ЭТАП 1: сбор ссылок ===")
    ensure_dirs()

    with sync_playwright() as p:
        context = create_browser_context(p)
        page = get_working_page(context)

        try:
            if not safe_goto(page, START_URL):
                return

            seen_pages: set[str] = set()
            parsed_pages = 0

            while parsed_pages < MAX_LINK_PAGES:
                current_url = page.url

                if current_url in seen_pages:
                    print("Эта страница уже была обработана. Останавливаю сбор ссылок.")
                    break

                seen_pages.add(current_url)
                parsed_pages += 1

                page.wait_for_timeout(random.randint(*INITIAL_WAIT_MS))

                if SCROLL_LISTING_PAGE:
                    scroll_page(page, LISTING_SCROLL_STEPS, LISTING_SCROLL_DELAY_MS)

                html = get_page_html_with_manual_check(page, expected="listing")
                if html is None:
                    break

                rows = parse_links(html=html, source_page_number=parsed_pages)

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
                print(f"CSV: {LINKS_CSV}")
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

                if not safe_goto(page, next_url):
                    break

        finally:
            context.close()


# ============================================================
# ПАРСИНГ КАРТОЧКИ
# ============================================================

def get_first_text(soup: BeautifulSoup, selectors: list[str]) -> str | None:
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = normalize_text(node.get_text(" ", strip=True))
            if text:
                return text
    return None


def get_meta_content(soup: BeautifulSoup, *names: str) -> str | None:
    for name in names:
        node = soup.select_one(f'meta[property="{name}"]')
        if node and node.get("content"):
            return normalize_text(node.get("content"))

        node = soup.select_one(f'meta[name="{name}"]')
        if node and node.get("content"):
            return normalize_text(node.get("content"))

    return None


def get_canonical_url(soup: BeautifulSoup) -> str | None:
    node = soup.select_one('link[rel="canonical"]')
    if node and node.get("href"):
        return normalize_text(node.get("href"))
    return None


def extract_json_ld(soup: BeautifulSoup) -> list[Any]:
    result: list[Any] = []

    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text()
        raw = raw.strip() if raw else ""
        if not raw:
            continue

        try:
            result.append(json.loads(raw))
        except Exception:
            continue

    return result


def extract_image_urls(soup: BeautifulSoup, json_ld_data: list[Any]) -> list[str]:
    urls: list[str] = []

    og_image = get_meta_content(soup, "og:image", "twitter:image")
    if og_image:
        urls.append(og_image)

    def collect_from_json_ld(value: Any) -> None:
        if isinstance(value, dict):
            image = value.get("image")
            if isinstance(image, str):
                urls.append(image)
            elif isinstance(image, list):
                for item in image:
                    collect_from_json_ld(item)

            for nested_value in value.values():
                if isinstance(nested_value, (dict, list)):
                    collect_from_json_ld(nested_value)

        elif isinstance(value, list):
            for item in value:
                collect_from_json_ld(item)

        elif isinstance(value, str) and ("avito" in value or value.startswith("http")):
            if re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", value, re.IGNORECASE):
                urls.append(value)

    collect_from_json_ld(json_ld_data)

    for img in soup.select("img"):
        src = img.get("src") or img.get("data-src")
        if src:
            full_url = urljoin(BASE_URL, str(src))
            if "avito" in full_url:
                urls.append(full_url)

    clean_urls: list[str] = []
    seen: set[str] = set()

    for url in urls:
        url = normalize_text(url) or ""
        if not url or url in seen:
            continue
        seen.add(url)
        clean_urls.append(url)

    return clean_urls[:30]


def parse_params(soup: BeautifulSoup) -> dict[str, str]:
    params: dict[str, str] = {}
    candidates: list[Tag] = []

    selectors = [
        '[data-marker*="item-view/item-params"] li',
        '[data-marker*="item-view/params"] li',
        '[data-marker*="item-params"] li',
        '[class*="params"] li',
        '[class*="Params"] li',
        "li",
    ]

    for selector in selectors:
        for node in soup.select(selector):
            if isinstance(node, Tag):
                candidates.append(node)

    seen_texts: set[str] = set()

    for node in candidates:
        text = normalize_text(node.get_text(" ", strip=True))
        if not text or text in seen_texts:
            continue

        seen_texts.add(text)

        if ":" not in text:
            continue

        if len(text) > 300:
            continue

        key, value = text.split(":", 1)
        key = normalize_text(key)
        value = normalize_text(value)

        if not key or not value:
            continue

        if len(key) > 100:
            continue

        params[key] = value

    return params


def slug_column_name(value: str) -> str | None:
    value = normalize_text(value)
    if not value:
        return None

    value = value.lower().replace("ё", "е")
    value = re.sub(r"[^\wа-я]+", "_", value, flags=re.IGNORECASE)
    value = value.strip("_")

    if not value:
        return None

    return f"param_{value[:60]}"


def detect_page_status(soup: BeautifulSoup) -> str:
    body_text = normalize_multiline(soup.get_text("\n", strip=True)) or ""
    lowered = body_text.lower()

    if "объявление снято" in lowered:
        return "removed"
    if "объявление удалено" in lowered:
        return "removed"
    if "объявление не найдено" in lowered or "страница не найдена" in lowered:
        return "not_found"

    return "ok"


def detect_seller_type(soup: BeautifulSoup, params: dict[str, str]) -> str | None:
    seller_type = get_first_text(
        soup,
        [
            '[data-marker="seller-info/label"]',
            '[data-marker="seller-info/summary"]',
            '[data-marker="user-info/label"]',
        ],
    )

    if seller_type:
        return seller_type

    text = normalize_multiline(soup.get_text("\n", strip=True)) or ""
    lowered = text.lower()

    markers = [
        "агентство",
        "застройщик",
        "собственник",
        "частное лицо",
        "компания",
    ]

    for marker in markers:
        if marker in lowered:
            return marker

    for key, value in params.items():
        combined = f"{key} {value}".lower()
        for marker in markers:
            if marker in combined:
                return marker

    return None


def parse_detail(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    json_ld_data = extract_json_ld(soup)
    image_urls = extract_image_urls(soup, json_ld_data)

    title = get_first_text(
        soup,
        [
            "h1",
            '[data-marker="item-view/title-info"]',
            '[data-marker="item-view/title"]',
        ],
    )

    if not title:
        title = get_meta_content(soup, "og:title", "twitter:title")

    object_type, space = parse_title_parts(title)

    price = get_first_text(
        soup,
        [
            '[data-marker="item-view/item-price"]',
            '[data-marker="item-price"]',
            '[itemprop="price"]',
        ],
    )
    price = clean_price(price)

    address = get_first_text(
        soup,
        [
            '[data-marker="item-view/item-address"]',
            '[data-marker="delivery/location"]',
            '[itemprop="address"]',
            '[class*="address"]',
        ],
    )

    description = get_first_text(
        soup,
        [
            '[data-marker="item-view/item-description"]',
            '[itemprop="description"]',
        ],
    )

    if not description:
        description = get_meta_content(soup, "og:description", "description", "twitter:description")

    seller_name = get_first_text(
        soup,
        [
            '[data-marker="seller-info/name"]',
            '[data-marker="user-info/name"]',
            '[data-marker="seller-info/label"]',
        ],
    )

    metro = get_first_text(
        soup,
        [
            '[data-marker="metro_link"]',
            '[data-marker*="metro"]',
        ],
    )

    params = parse_params(soup)
    seller_type = detect_seller_type(soup, params)

    row: dict[str, Any] = {
        "item_id": extract_item_id(url),
        "page_status": detect_page_status(soup),
        "title": title,
        "object_type": object_type,
        "space": space,
        "price": price,
        "address": address,
        "metro": metro,
        "seller_name": seller_name,
        "seller_type": seller_type,
        "description": description,
        "canonical_url": get_canonical_url(soup),
        "image_count": str(len(image_urls)),
        "image_urls_json": json.dumps(image_urls, ensure_ascii=False),
        "params_json": json.dumps(params, ensure_ascii=False),
        "json_ld_json": json.dumps(json_ld_data, ensure_ascii=False)[:20_000],
        "full_page_text": None,
        "collected_at": now_iso(),
        "url": url,
    }

    for key, value in params.items():
        column_name = slug_column_name(key)
        if column_name and column_name not in row:
            row[column_name] = value

    if SAVE_FULL_PAGE_TEXT:
        full_page_text = normalize_multiline(soup.get_text("\n", strip=True))
        if full_page_text:
            row["full_page_text"] = full_page_text[:FULL_TEXT_LIMIT]

    return row


def get_links_for_details() -> list[str]:
    links_df = load_csv(LINKS_CSV, LINK_COLUMNS)

    if links_df.empty or "url" not in links_df.columns:
        return []

    details_df = load_csv(DETAILS_CSV, DETAIL_BASE_COLUMNS)

    already_done: set[str] = set()
    if not details_df.empty and "url" in details_df.columns:
        already_done = set(details_df["url"].dropna().astype(str))

    urls: list[str] = []

    for url in links_df["url"].dropna().astype(str):
        url = url.strip()
        if not url:
            continue
        if url in already_done:
            continue
        urls.append(url)

    return urls[:MAX_DETAIL_CARDS]


def collect_details() -> None:
    print("\n=== ЭТАП 2: сбор подробной информации ===")
    ensure_dirs()

    urls = get_links_for_details()

    if not urls:
        print("Нет новых ссылок для подробного парсинга.")
        return

    print(f"Будет обработано карточек: {len(urls)}/{MAX_DETAIL_CARDS}")

    with sync_playwright() as p:
        context = create_browser_context(p)
        page = get_working_page(context)

        try:
            for index, url in enumerate(urls, start=1):
                print(f"\nКарточка {index}/{len(urls)}")
                print(url)

                if not safe_goto(page, url):
                    continue

                page.wait_for_timeout(random.randint(*INITIAL_WAIT_MS))

                if SCROLL_DETAIL_PAGE:
                    scroll_page(page, DETAIL_SCROLL_STEPS, DETAIL_SCROLL_DELAY_MS)

                html = get_page_html_with_manual_check(page, expected="detail")
                if html is None:
                    break

                try:
                    row = parse_detail(html, url)
                except Exception as exc:
                    print(f"Ошибка парсинга карточки: {exc!r}")
                    save_debug_page(page, reason="detail_parse_error")
                    continue

                upsert_rows(
                    rows=[row],
                    csv_path=DETAILS_CSV,
                    excel_path=DETAILS_EXCEL,
                    columns=DETAIL_BASE_COLUMNS,
                    key_column="url",
                    sheet_name="details",
                )

                print(f"Сохранено: {DETAILS_CSV}")
                print(f"Excel-дубликат: {DETAILS_EXCEL}")

                sleep_random(CARD_DELAY_SECONDS)

        finally:
            context.close()


# ============================================================
# ЗАПУСК
# ============================================================

def main() -> None:
    if RUN_MODE not in {"links", "details", "all"}:
        raise ValueError('RUN_MODE должен быть "links", "details" или "all"')

    if RUN_MODE in {"links", "all"}:
        collect_links()

    if RUN_MODE in {"details", "all"}:
        collect_details()


if __name__ == "__main__":
    main()
