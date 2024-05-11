

from RawMaterials.get_data_by_selenium_obj import WebScraper

class YekanGetDataObjects(WebScraper):
    def __init__(self):
        super().__init__()

    def select_fund_yekan(self, visible_text):
        """
        این تابع برای انتخاب -صندوق- در نرم‌افزار یکان درست شده است.
        لیست موارد انتخابی عبارتند از
        []
        :param visible_text: گزینه انتخابی
        :return: این یک تابع عملکردی است
        """
        tag_select1 = '//*[@id="PortfolioIds"]'
        tag_select2 = '//*[@id="PortfolioIds"]'

        try:
            self.select_one_item(tag_select1, visible_text)
        except:
            self.select_one_item(tag_select2, visible_text)

        print(f"Selected {visible_text}")

    def select_tajmi(self, visible_text):
        """
        این تابع برای انتخاب -تجمیع- در نرم‌افزار یکان درست شده است.
        لیست موارد انتخابی عبارتند از
        []
        :param driver:
        :param visible_text: گزینه انتخابی
        :return: این یک تابع عملکردی است
        """

        tag_select = '//*[@id="BuySellReportGroupType"]'
        self.select_one_item(tag_select, visible_text)

    def select_tafkik(self, visible_text):
        """
        این تابع برای انتخاب -تفکیک بر اساس- در نرم‌افزار یکان به کار می رود. لیست موارد انتخابی عبارتند از
        ["بلی","خیر"]
        :param visible_text: گزینه انتخابی
        :param driver:
        :return: این یک تابع عملکردی است
        """
        tag_select = '//*[@id="SeparateByPortfolio"]'
        self.select_one_item(tag_select, visible_text)

    def select_date_yekan(self, start_date=None, end_date=None):
        """
        این تابع برای وارد کردن تاریخ‌های شروع و پایان گزارش گیری در نرم‌افزار یکان به کار می‌رود
        :param start_date: تاریخ شروع
        :param end_date: تاریخ پایان
        """
        if start_date is not None:
            tag_start_date = '//*[@id="StartDateTime"]'
            self.select_date(start_date, tag_start_date)

        if end_date is not None:
            tag_end_date = '//*[@id="EndDateTime"]'
            self.select_date(end_date, tag_end_date)

    def download_data_yekan(self, folder_name, default_name, custom_name):
        """
        این تابع برای دانلود گزارش های لازم به صورت اکسل از نرم‌افزار یکان به کار می رود

        :param name2:
        :param name1:
        :param driver:
        :param main_path: مسیر اصلی پروژه
        :return: این یک تابع عملکردی است
        """
        download_button_tag = '//*[@id="export-btn"]/button'
        download_tag = '//*[@id="export"]/li[1]/a'
        self.download_data(download_tag, folder_name, default_name, custom_name)