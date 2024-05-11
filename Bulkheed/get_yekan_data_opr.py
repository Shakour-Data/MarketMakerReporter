# developed by: Shakour Alishahi
# ======================================================================================================================
# ######################################################################################################################
# import external libraries
# ----------------------------------------------------------------------------------------------------------------------


# ======================================================================================================================
# ######################################################################################################################
# import internal libraries
# ----------------------------------------------------------------------------------------------------------------------

# ======================================================================================================================
# ######################################################################################################################
# Database call
# ----------------------------------------------------------------------------------------------------------------------


# ======================================================================================================================
# ######################################################################################################################
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from RawMaterials.data_base_obj import DataHelper

from Materials.get_data_yekan_obj import YekanGetDataObjects
from Materials.UserNames_PassWords import UserNamePassword

class GetYekanData(YekanGetDataObjects, DataHelper):

    def __init__(self):
        super().__init__()
        self.main_url = 'https://manage.sinabehgozin.ir/'
        self.user_name, self.pass_word = UserNamePassword.yekan_user_pass()

    def enter_site_yekan(self):
        self.navigate_to_page(f'{self.main_url}/login?ReturnUrl=%2f')
        username = WebDriverWait(self.browser, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="Username"]')))
        username.send_keys(self.user_name)

        psswrd = WebDriverWait(self.browser, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="Password"]')))
        psswrd.send_keys(self.pass_word)

        print("Please Enter Captcha")
        login_site = self.browser.find_element(By.XPATH, '/html/body/div/div[2]/div[1]/form/div[6]/button')
        # login_site.click()

        time.sleep(15)
        # self.close_browser()

    def get_Mojoudi_yekan(self):
        # ۱- وارد شدن به صفحه
        sub_page_path = "reports/asset/events"
        self.sub_page(self.main_url, sub_page_path)
        time.sleep(15)

    def get_dailyreport_yekan(self, date_list):
        try:
            # ۱- وارد شدن به صفحه
            sub_page_path = "reports/portfolio/dailyreport"
            self.sub_page(self.main_url, sub_page_path)
            time.sleep(5)
            # ۲- انتخاب گزینه -همه- در بخش انتخاب صندوق
            visible_text = "همه"
            self.select_fund_yekan(visible_text)
            time.sleep(3)
            for date in date_list:
                self.select_date_yekan(end_date=date)
                folder_name = "DailyReports"
                default_name = "Report.xlsx"
                custom_name = f"DailyReportYekan_{date}.xlsx"
                self.download_data_yekan(folder_name, default_name, custom_name)
                print(f"DailyReportYekan_{date}.xlsx downloaded")
                time.sleep(5)
        except Exception as e:
            print(f"An error occurred: {e}")
            # در اینجا می‌توانید اقدامات مرتبط با خطا را انجام دهید.

    def get_buysell_Yekan(self, start_date):
        """
        برای دانلود دیتای خرید و فروش طی دوره از سایت یکان---
        گزارش / دارایی / خرید و فروش طی دوره---
        https://manage.sinabehgozin.ir/reports/asset/buysellreportevents
        :param main_path:
        :param main_html:
        :param start_date:
        :param time_sleep:
        :return: این یک تابع عملکردی است
        """
        # ۱- وارد شدن به صفحه
        sub_page_path = "reports/asset/buysellreportevents"
        self.sub_page(self.main_url, sub_page_path)
        time.sleep(3)
        # ۲- انتخاب گزینه -همه- در بخش انتخاب صندوق
        visible_text = "همه"
        self.select_fund_yekan(visible_text)
        time.sleep(3)
        # ۳- انتخاب گزینه -روز- در بخش تجمیع بر اساس
        visible_text = 'روز'
        self.select_tajmi(visible_text)
        time.sleep(3)
        # ۴- انتخاب گزینه -بلی- در بخش انتخاب تفکیک بر اساس صندوق
        visible_text = 'بلی'
        self.select_tafkik(visible_text)
        time.sleep(3)
        # ۵- وارد کردن تاریخ گزارش و فشردن کلید اینتر
        self.select_date_yekan(start_date=start_date, end_date=self.j_today)
        time.sleep(3)
        # ۶- دانلود فایل
        folder_name = "BuySell"
        default_name = "Report.xlsx"
        custom_name = f"BuySell_{self.j_today}.xlsx"
        self.download_data_yekan(folder_name, default_name, custom_name)
        print(f"BuySell_{self.j_today}.xlsx downloaded")
# ======================================================================================================================
    def get_request_issuance_cancellation_Yekan(self):
        # ۱- وارد شدن به صفحه
        sub_page_path = "manage/portfolio/purchaseredemptionrequest/list"
        self.sub_page(self.main_url, sub_page_path)
        time.sleep(3)

        # ۲- دانلود فایل
        folder_name = "IssuanceCancellation"
        default_name = "Report.xlsx"
        custom_name = f"RequestIssuanceCancellation.xlsx"
        self.download_data_yekan(folder_name, default_name, custom_name)
        print(f"RequestIssuanceCancellation.xlsx downloaded")
    def get_issuance_cancellation_Yekan(self):
        # ۱- وارد شدن به صفحه
        sub_page_path = "manage/portfolio/purchaseredemption/list"
        self.sub_page(self.main_url, sub_page_path)
        time.sleep(3)

        # ۲- دانلود فایل
        folder_name = "IssuanceCancellation"
        default_name = "Report.xlsx"
        custom_name = f"IssuanceCancellation.xlsx"
        self.download_data_yekan(folder_name, default_name, custom_name)
        print(f"IssuanceCancellation.xlsx downloaded")




