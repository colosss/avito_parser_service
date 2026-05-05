from selenium import webdriver
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import random
from time import sleep
import pandas as pd
import subprocess

PAGES = 50
SAVE_EVERY=50
FILE_NAME="final_avito_dataset_"

def normalize_text(txt:str):
    return txt.replace('\xa0', ' ').replace('\u202f', ' ')

def normilize_time(time:str):
    time=normalize_text(txt=time)
    time=time.replace('–',' ')
    parts=[x for x in time.split(' ')]
    return parts[1] if time else None



def plus_null(d:str):
    parts_d=[x.strip() for x in d.split('.')]
    if len(parts_d[0])<2:
        parts_d[0]='0'+parts_d[0]
        return '.'.join(parts_d) if parts_d else None
    return d

def normalize_d(d:str):
    month={
        'января': '01',
        'февраля': '02',
        'марта': '03',
        'апреля': '04',
        'мая': '05',
        'июня': '06',
        'июля': '07',
        'августа': '08',
        'сентября': '09',
        'октября': '10',
        'ноября': '11',
        'декабря': '12',
    }

    time={
        'сегодня': pd.Timestamp.now().strftime('%d.%m.%Y'),
        'вчера': (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%d.%m.%Y'),
        'сейчас': pd.Timestamp.now().strftime('%d.%m.%Y'),
    }
    d=normalize_text(txt=d).lower()
    parts_d=[x.strip() for x in d.split(' ')]
    if len(parts_d)<=1:
        if d in time:
            return time[d]
        else:   
            return d
    else:
        index=1
    
    for key, value in month.items():
        if key in parts_d[index]:
            if ':' in parts_d[2]:
                d=parts_d[0]+'.'+value+'.'+pd.Timestamp.now().strftime('%Y')
            else:
                d=d.replace(key, value)
            return d.replace(' ', '.')
        
    if 'недел' in parts_d[index]:
        return (pd.Timestamp.now() - pd.Timedelta(days=7*int(parts_d[0]))).strftime('%d.%m.%Y')
    elif 'месяц' in parts_d[index]:
        return (pd.Timestamp.now() - pd.Timedelta(days=30*int(parts_d[0]))).strftime('%d.%m.%Y')
    elif 'год' in parts_d[index]:
        return (pd.Timestamp.now() - pd.Timedelta(days=365*int(parts_d[0]))).strftime('%d.%m.%Y')
    elif 'дн' in parts_d[index]:
        return (pd.Timestamp.now() - pd.Timedelta(days=int(parts_d[0]))).strftime('%d.%m.%Y')
    elif 'час' in parts_d[index]:
        return (pd.Timestamp.now() - pd.Timedelta(hours=int(parts_d[0]))).strftime('%d.%m.%Y')
    elif 'минут' in parts_d[index]:
        return (pd.Timestamp.now() - pd.Timedelta(minutes=int(parts_d[0]))).strftime('%d.%m.%Y')
    elif 'секунд' in parts_d[index]:
        return (pd.Timestamp.now() - pd.Timedelta(seconds=int(parts_d[0]))).strftime('%d.%m.%Y')
    elif 'лет' in parts_d[index]:
        return (pd.Timestamp.now() - pd.Timedelta(days=365*int(parts_d[0]))).strftime('%d.%m.%Y')
    
    return d

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


options = uc.ChromeOptions()
options.binary_location = chrome_path
options.add_argument("--no-sandbox")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-setuid-sandbox")
options.add_argument("--no-zygote")
options.add_argument("--disable-gpu")
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
        if len(parts)>1:
            parts[1]=normalize_text(txt=parts[1])
            if len(parts[1])>1:
                space_part=[x.strip() for x in parts[1].split(' ')]
                parts[1]=space_part[0]

        # if len(parts)<2:
        #     continue

        price_tag=card.find('span', {'data-marker': 'item-price-value'})
        price=price_tag.get_text(strip=True) if price_tag else None
        price = normalize_text(txt=price.replace('от', '').strip())
        # if not price:
        #     continue

        area_price=None
        for p in card.find_all('p'):
            txt=p.get_text(strip=True)
            txt=normalize_text(txt=txt)
            if txt and ('за м²' in txt or 'за м2' in txt):
                area_price = txt.replace('за м²', '').replace('за м2', '').strip()
        # if area_price is None:
        #     continue
            
        street=card.find('a', {'data-marker': 'street_link'})
        house=card.find('a', {'data-marker': 'house_link'})
        address=f"{street.get_text(strip=True) if street else ''}, {house.get_text(strip=True) if house else ''}".strip(', ')
        if not address:
            continue

        metro_link=card.find('a', {'data-marker': 'metro_link'})
        metro_name=metro_link.get_text(strip=True) if metro_link else ''
        # if not metro_name:
        #     continue

        time_to_metro=""
        if metro_link:
            next_span=metro_link.find_next_sibling('span')
            if next_span:
                time_to_metro=next_span.get_text(strip=True)
                time_to_metro=time_to_metro.strip(',')
        time_to_metro=normilize_time(time=time_to_metro)
        # if not time_to_metro:
        #     continue

        d_tag=card.find('div', {'data-marker': 'item-date/wrapper'})
        d=d_tag.get_text(strip=True) if d_tag else None
        d=normalize_d(d=d) if d else None
        d=plus_null(d=d) if d else None
        # if not d:
        #     continue

        link_tag=card.find('a', {'data-marker': 'item-title'})
        source=f"https://www.avito.ru{link_tag.get('href')}" if link_tag else None
        # if not source:
        #     continue

        data = {
            'type': parts[0] if len(parts) > 0 else None,
            'space': parts[1] if len(parts) > 1 else None,
            'price': price if price else None,
            'area_price': area_price if area_price else None, 
            'address': address if address else None,
            'metro': metro_name if metro_name else None,
            'time_to_metro': time_to_metro if time_to_metro else None,
            'date': d if d else None,
            'source': source if source else None
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