from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import random

BASE_URL = "https://www.avito.ru"

def parse_links(html):
    soup = BeautifulSoup(html, "html.parser")
    result = []

    for card in soup.find_all("div", {"data-marker": "item"}):
        a = card.find("a", {"data-marker": "item-title"})
        if not a:
            continue

        href = a.get("href")
        if not href:
            continue

        result.append(urljoin(BASE_URL, href))

    return result

def collect_links():
    # start_url = "https://www.avito.ru/"
    start_url = "https://www.avito.ru/sankt-peterburg/kvartiry/prodam-ASgBAgICAUSSA8YQ"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(locale="ru-RU")
        page = context.new_page()

        page.goto(start_url, wait_until="domcontentloaded")

        seen_pages = set()

        while True:
            current_url = page.url

            if current_url in seen_pages:
                break

            seen_pages.add(current_url)
            page.wait_for_timeout(random.randint(3000, 7000))
            html = page.content()

            if "geetest_captcha" in html.lower():
                print("Появилась проверка/капча. Приостанавливаю сбор. После решения капчи введите любой симовл в терминал")
                input()

            links = parse_links(html)
            print(f"Страница: {current_url}")
            print(f"Найдено ссылок: {len(links)}")

            # тут сохранить links в БД

            next_link = page.locator('a[aria-label*="Следующая"]').first

            if next_link.count() == 0:
                print("Следующей страницы нет.")
                break

            href = next_link.get_attribute("href")

            if not href:
                print("Не нашёл href следующей страницы.")
                break

            next_url = urljoin(BASE_URL, href)
            page.goto(next_url, wait_until="domcontentloaded")
            
            time.sleep(random.uniform(20, 60))

        browser.close()

if __name__ == "__main__":
    collect_links()