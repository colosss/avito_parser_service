from selenium import webdriver
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import random
from time import sleep
import pandas as pd
import subprocess

PAGES = 40
SAVE_EVERY=40
FILE_NAME="26_02_avito_parser_"

def normalize_text(txt:str):
    return txt.replace('\xa0', ' ').replace('\u202f', ' ')

chrome_path = None
for binary in ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser']:
    try:
        result = subprocess.check_output(['which', binary], stderr=subprocess.DEVNULL).decode('utf-8').strip()
        if result:
            chrome_path = result
            print(f"✅ Найден браузер: {chrome_path}")
            break
    except:
        continue

if not chrome_path:
    print("❌ Chrome/Chromium не найден!")
    print("Выполни: sudo dnf install chromium")
    exit(1)

# driver = webdriver.Chrome()

options = uc.ChromeOptions()
options.binary_location = chrome_path
options.add_argument("--no-sandbox")
options.add_argument("--disable-blink-features=AutomationControlled")
# options.add_argument("--disable-setuid-sandbox")
# options.add_argument("--no-zygote")
# options.add_argument("--disable-gpu")
driver = uc.Chrome(
    options=options,
    use_subprocess=True,
    version_main=145
)

all_data = []
for page in range(1, PAGES+1):
    url = f'https://www.avito.ru/sankt-peterburg/kvartiry/prodam?p={page}'
    print(f'Парсим страницу {page}/{PAGES}...')
    driver.get(url=url)

    sleep(random.uniform(8, 15))

    for _ in range(12):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(random.uniform(1.2, 2.8))

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    cards = soup.find_all('div', {'data-marker': 'item'})
    
    print(f'  Найдено карточек: {len(cards)}')
    
    for card in cards:
        title_tag=card.find('a', {'data-marker': 'item-title'})
        title_text=title_tag.get_text(strip=True) if title_tag else ''
        parts=[x.strip() for x in title_text.split(',')]
        if ' ' in parts[1]:
            part=[x.strip() for x in parts[1].split(' ')]
            parts[1]=part[0]

        price_tag=card.find('span', {'data-marker': 'item-price-value'})
        price=price_tag.get_text(strip=True) if price_tag else None

        area_price=None
        for p in card.find_all('p'):
            txt=p.get_text(strip=True)
            txt=normalize_text(txt=txt)
            if txt and ('за м²' in txt or 'за м2' in txt):
                # print(f"\nfind area_price - {area_price}\n")
                area_price = txt
                break
        
        street=card.find('a', {'data-marker': 'street_link'})
        house=card.find('a', {'data-marker': 'house_link'})
        address=f"{street.get_text(strip=True) if street else ''}, {house.get_text(strip=True) if house else ''}".strip(', ')

        metro_link=card.find('a', {'data-marker': 'metro_link'})
        metro_name=metro_link.get_text(strip=True) if metro_link else ''

        time_to_metro=""
        if metro_link:
            next_span=metro_link.find_next_sibling('span')
            if next_span:
                time_to_metro=next_span.get_text(strip=True)
                time_to_metro=time_to_metro.strip(',')
        # metro=f"{metro_name} {time_to_metro}".strip()

        # description=card.find('p', {'style': '--module-max-lines-size: 4;'})
        # subtitle=description.get_text(strip=True) if description else ''
        subtitle=''
        for p in card.find_all('p'):
            # subtitle_tag=p.get_attribute_list('style')
            # if subtitle_tag and (('--module-max-lines-size:\xa04;' in subtitle_tag) or ('--module-max-lines-size:\u202f4;' in subtitle_tag) or ('--module-max-lines-size:\u202f4;' in subtitle_tag)):
            #     print(f"\nfind subtitle - {p.get_text(strip=True)}\n")
            #     subtitle = p.get_text(strip=True)
            #     break
            style=p.get('style', '')
            if '--module-max-lines-size' in style and '4' in style:
                subtitle = p.get_text(strip=True)
                break
            
        date_tag=card.find('div', {'data-marker': 'item-date/wrapper'})
        date=date_tag.get_text(strip=True) if date_tag else None

        link_tag=card.find('a', {'data-marker': 'item-title'})
        source=f"https://www.avito.ru{link_tag.get('href')}" if link_tag else None
        data = {
            'type': parts[0] if len(parts) > 0 else None,
            'space': parts[1] if len(parts) > 1 else None,
            'price': price,
            'area_price': area_price,
            'address': address,
            'metro': metro_name,
            'time_to_metro': time_to_metro,
            'subtitle': subtitle[:500] + '...' if len(subtitle) > 500 else subtitle,
            'date': date,
            'source': source
        }
        
        all_data.append(data)

        if page%SAVE_EVERY==0 or page==PAGES:
            df = pd.DataFrame(all_data)
            filename = f'{FILE_NAME}{page}_pages.csv'
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f'Сохранено {len(all_data)} строк -> {filename}\n')

driver.quit()

df = pd.DataFrame(all_data)
df.to_csv('avito_data.csv', index=False, encoding='utf-8-sig')
print(f'Готово! Всего собрано: {len(all_data)} объявлений')