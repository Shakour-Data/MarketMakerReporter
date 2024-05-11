
import os
import shutil
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

class WebScraper:
    def __init__(self):
        self.browser = webdriver.Chrome()
        self.project_path = "/home/shakour/shakour/Programming/Codes/GitStudy/MarketMakerReporter"
        self.main_path = "Warehouse/YekanWarehouse/YekanFiles/"

    def navigate_to_page(self, url):
        self.browser.get(url)

    def close_browser(self):
        self.browser.quit()

    def sub_page(self, main_html, sub_page_path):
        """
        این تابع برای ورود به یکی از صفحات سایت به کار می رود

        :param main_html: آدرس صفحه اصلی سایت
        :param sub_page_path: آدرس صفحه سایت
        :return:
        """
        self.browser.get(main_html + sub_page_path)
        time.sleep(15)

    def select_one_item(self, tag_select, visible_text):
        """
        این تابع برای اتخاب یک گزینه از گزینه های اراپه شده در بخش انتخاب به کار می رود

        :param tag_select: XPATH
        :param visible_text: گزینه انتخابی
        :return:
        """
        tag_select_funds = Select(self.browser.find_element(By.XPATH, tag_select))
        tag_select_funds.select_by_visible_text(visible_text)

    def select_date(self, date, tag_date):
        date_ = date.replace('-', '/')
        date_area = self.browser.find_element(By.XPATH, tag_date)
        date_area.clear()
        time.sleep(2)
        date_area.send_keys(date_)
        time.sleep(2)
        date_area.send_keys(Keys.ENTER)

    def download_data(self, download_tag, folder_name, default_name, custom_name):
        defult_download_path = '/home/shakour/Downloads'

        for filename in os.listdir(defult_download_path):
            file_path = os.path.join(defult_download_path, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"File {file_path} deleted successfully!")
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")

        download_button_tag = '//*[@id="export-btn"]/button'
        time.sleep(5)
        self.browser.find_element(By.XPATH, download_button_tag).click()
        download_path = f'{self.project_path}/{self.main_path}/{folder_name}'
        params = {'behavior': 'allow', 'downloadPath': download_path}
        self.browser.execute_cdp_cmd('Page.setDownloadBehavior', params)
        try:
            if "/a" not in download_button_tag:
                time.sleep(5)
                self.browser.find_element(By.XPATH, download_tag).click()
        except:
            pass
        source_path = f'{defult_download_path}/{default_name}'  # Replace with your source file path
        destination_path = download_path  # Replace with your destination file path

        # Check if the source file exists
        if os.path.exists(source_path):
            # and not os.path.exists(f'{self.main_path}/{folder_name}/{custom_name}')
            # Moving the file from the source path to the destination path
            shutil.move(source_path, destination_path)
            print("File moved successfully!")
        else:
            print("Either the source file doesn't exist or the custom file already exists.")

        time.sleep(10)
        os.rename(f'{self.project_path}/{self.main_path}/{folder_name}/{default_name}',
                  f'{self.project_path}/{self.main_path}/{folder_name}/{custom_name}')
