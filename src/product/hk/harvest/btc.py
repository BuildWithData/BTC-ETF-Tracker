from datetime import datetime
import os
from pathlib import Path
from product.abc import ETP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from time import sleep


class HB9439(ETP):
    """Harvest Global Investments"""

    def url(self):
        return "https://www.harvestglobal.com.hk"

    def _file_extension(self):
        return "html"

    def scrape(self):

        timestamp = datetime.today()

        options = Options()
        #options.add_argument('--headless')
        driver = webdriver.Chrome(options)

        driver.get(self.url())
        sleep(10)
        driver.find_elements(By.CLASS_NAME, "button-box")[6].click()

        sleep(5)
        driver.execute_script("window.scrollTo(0, 1200)")
        sleep(3)
        driver.find_element(By.ID, "Our-Strategies").find_element(By.CLASS_NAME, "button-box").click()

        sleep(5)
        driver.execute_script("window.scrollTo(0, 1000)")
        sleep(3)
        driver.find_element(By.NAME, 'Accept').click()

        sleep(5)
        driver.find_elements(By.CLASS_NAME, "row")[0].find_elements(By.TAG_NAME, "span")[3].click()

        sleep(5)
        section = driver.find_element(By.ID, "fundvirtualasset-etf").find_element(By.XPATH, '..')
        section.find_elements(By.TAG_NAME, "span")[1].click()

        sleep(5)
        driver.execute_script("window.scrollTo(0, 1500)")
        market_information = [t for t in driver.find_elements(By.TAG_NAME, 'h3') if "market information" in t.text.lower()][0]
        usd_button = [s for s in market_information.find_elements(By.TAG_NAME, "span") if "USD" in s.text][0]
        usd_button.click()

        sleep(5)
        path = os.path.join(self.path(), self._file_name(timestamp))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            f.write(driver.page_source)

    def extract(self):
        raise NotImplementedError

    def update_db(self):
        raise NotImplementedError
