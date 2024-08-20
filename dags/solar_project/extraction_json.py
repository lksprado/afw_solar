from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import Select
from airflow.models import Variable
import requests
import os
import logging
import time
import datetime
import json


class EMAWebScraper:
    def __init__(self):
        self.driver = None
        self.username = Variable.get("apsystem_user")
        self.password = Variable.get("apsystem_pw")
        self.base_url = "https://apsystemsema.com/ema/index.action"
        self.cookies = None
        self.user_id = None
        self.missing_file = "/opt/airflow/data/solar_project/missing_dates.csv"

    # IDENTIFY MISSING DAYS AND APPEND TO LIST
    def read_missing(self):
        days = []
        with open(self.missing_file, "r") as file:
            for line in file:
                days.append(line.split(",")[0].strip())
        return days

    # SETTING UP WEBDRIVER
    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--remote-debugging-port=9222")
        # Connect to the Selenium container
        selenium_url = "http://selenium_container:4444/wd/hub"
        self.driver = webdriver.Remote(
            command_executor=selenium_url, options=chrome_options
        )

    # LOGGING IN
    def login(self):
        try:
            self.driver.get(self.base_url)
            username_field = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, "username"))
            )
            password_field = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, "password"))
            )
            username_field.send_keys(self.username)
            password_field.send_keys(self.password)
            login_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, "Login"))
            )
            login_button.click()
            time.sleep(3)
        except TimeoutException as e:
            logging.error(
                "Timeout occurred while waiting for an element to be clickable: %s",
                str(e),
            )
        except Exception as e:
            logging.error("An unexpected error occurred: %s", str(e))

    # GETTING THE AJAX
    def ajax_finder(self):
        report_button = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.ID, "report_head"))
        )
        report_button.click()

        system_data_button = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.ID, "systemDataCustomer"))
        )
        system_data_button.click()

        ecu_data = WebDriverWait(self.driver, 15).until(
            EC.element_to_be_clickable((By.ID, "ecuData"))
        )
        ecu_data.click()

        # WAIT FOR IFRAME AND SWITCH TO IT
        iframe = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="configuration_body"]'))
        )
        self.driver.switch_to.frame(iframe)

        select = Select(self.driver.find_element(By.ID, "chart"))

        select.select_by_value("2")

        time.sleep(3)

        # SWITCH BACK TO DEFAULT CONTENT
        self.driver.switch_to.default_content()

    # REQUESTING THROUGH AJAX
    def fetch_production_data(self, query_date):
        try:
            self.cookies = self.driver.get_cookies()
            headers = {
                "Cookie": "; ".join(
                    [f"{cookie['name']}={cookie['value']}" for cookie in self.cookies]
                ),
                "User-Agent": "Mozilla/5.0 (Windowstime.sleep(3) NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            }
            url = "https://apsystemsema.com/ema/ajax/getReportApiAjax/getHourlyEnergyOnCurrentDayAjax"

            for cookie in self.cookies:
                if cookie["name"] == "userId":
                    self.user_id = cookie["value"]

            payload = {
                "selectedValue": "216200001531",
                "queryDate": query_date,
                "systemId": self.user_id,
                "userId": self.user_id,
            }
            response = requests.post(url, headers=headers, data=payload)

            file_date = f"{query_date[:4]}-{query_date[4:6]}-{query_date[6:]}"

            output_file = os.path.join(
                "/opt/airflow/data/solar_project/json_files",
                f"hourly24_production_{file_date}.json",
            )

            with open(output_file, "w") as f:
                json.dump(response.json(), f)
        except requests.exceptions.RequestException as e:
            logging.error("Error making HTTP request: %s", str(e))
        except Exception as e:
            logging.error("An error occurred: %s", str(e))

    def run(self):
        # CHECK FOR DATES DO SCRAP
        days = self.read_missing()

        if not days:
            logging.info("No missing days to process.")
            return  # EXIT IF NOTHING IS FOUND

        self.setup_driver()
        self.login()
        self.ajax_finder()

        max_date = max(days)

        for day in days:
            if day > max_date:
                break
            query_date = datetime.datetime.strptime(day, "%Y-%m-%d").strftime("%Y%m%d")
            self.fetch_production_data(query_date)

        self.driver.quit()


def main():
    scraper = EMAWebScraper()
    scraper.run()
