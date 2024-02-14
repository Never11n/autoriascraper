import os
import requests
import psycopg2
import schedule
import subprocess
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

load_dotenv()


def run_function():
    schedule.every().day.at(os.getenv('START_TIME')).do(create_dump)
    schedule.every().day.at(os.getenv('START_TIME')).do(scrape_auto_ria)
    while True:
        schedule.run_pending()


def create_dump():
    dump_path = os.path.join(os.getcwd(), 'dumps', 'dump.sql')
    dump_command = f"pg_dump -U {os.getenv('DB_USER')} -d {os.getenv('DB_NAME')} > {dump_path}"
    subprocess.run(dump_command, shell=True)


def scrape_auto_ria():
    base_url = f"https://auto.ria.com/uk/legkovie/?page="
    page_num = 1
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    while True:
        url = base_url + str(page_num)

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            continue

        soup = BeautifulSoup(response.content, "html.parser")
        listings = soup.find_all("div", class_="content-bar")
        if not listings:
            print('the last listing was reached')
            break

        for listing in listings:

            listing_link = listing.find("a", class_="address").get("href")

            driver = webdriver.Edge()
            driver.get(listing_link)
            driver.execute_script("window.scrollBy(0, 400);")

            # Загружаем страницу объявления
            listing_response = requests.get(listing_link, headers=headers)
            listing_soup = BeautifulSoup(listing_response.content, "html.parser")

            # Извлекаем нужные данные
            title = listing_soup.find("h3", class_="auto-content_title").text.strip()
            price_usd_raw = listing_soup.find("span", class_="price_value").text.strip()
            price_usd = int(''.join(filter(str.isdigit, price_usd_raw)))
            odometer_raw = listing_soup.find("div", class_="bold").text.strip()
            odometer = int(''.join(filter(str.isdigit, odometer_raw)) + '000')
            username = listing_soup.find("div", class_="seller_info_name")
            if username:
                username = username.text.strip()
            image_url = listing_soup.find('img', class_='outline m-auto').get('src')
            image_count_text = listing_soup.find('span', class_="count").find('span', class_="mhide").text.strip()
            image_count = int(''.join(filter(str.isdigit, image_count_text)))
            car_vin = listing_soup.find('span', class_='label-vin')
            if car_vin:
                car_vin = car_vin.text.strip()
            car_number = listing_soup.find('span', class_='state-num ua')
            if car_number:
                car_number = car_number.text.strip()

            link = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "phone_show_link"))
            )
            link.click()
            phone_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "popup-successful-call-desk"))
            )
            phone_number = "+38" + phone_element.get_attribute("data-value").strip()
            driver.close()
            save_to_db(url=listing_link,
                       title=title,
                       price_usd=price_usd,
                       odometer=odometer,
                       username=username,
                       phone_number=phone_number,
                       image_url=image_url,
                       images_count=image_count,
                       car_number=car_number,
                       car_vin=car_vin)

        page_num += 1


def save_to_db(url, title, price_usd, odometer, username, phone_number, image_url, images_count, car_number, car_vin):
    print(os.getenv('DB_NAME'))
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS cars (
    id SERIAL PRIMARY KEY,
    url TEXT,
    title TEXT,
    price_usd NUMERIC,
    odometer NUMERIC,
    username TEXT,
    phone_number TEXT,
    image_url TEXT,
    images_count INTEGER,
    car_number TEXT,
    car_vin TEXT,
    datetime_found TIMESTAMP
    );
    """)

    cur.execute("SELECT EXISTS(SELECT 1 FROM cars WHERE url = %s)", (url,))
    exists = cur.fetchone()[0]

    if not exists:
        cur.execute("""
            INSERT INTO cars (url, title, price_usd, odometer, username, phone_number, image_url, images_count, car_number, car_vin, datetime_found)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (url, title, price_usd, odometer, username, phone_number, image_url, images_count, car_number, car_vin))
        conn.commit()
    else:
        logging.info("Запись с таким URL уже существует")

    cur.close()
    conn.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Скрипт запущен успешно')

    scrape_auto_ria()
    run_function()
