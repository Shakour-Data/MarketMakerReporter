# developed by: Shakour Alishahi
# ======================================================================================================================
# ######################################################################################################################
# import external libraries
# ----------------------------------------------------------------------------------------------------------------------

import sqlite3
# ======================================================================================================================
# ######################################################################################################################
# import internal libraries
# ----------------------------------------------------------------------------------------------------------------------

from RawMaterials.data_base_obj import DataHelper
from Bulkheed.get_yekan_data_opr import GetYekanData

# ======================================================================================================================
# ######################################################################################################################
# Database call
# ----------------------------------------------------------------------------------------------------------------------


# ======================================================================================================================
# ######################################################################################################################

class FetchYekanData(GetYekanData, DataHelper):
    def __init__(self, start_report):
        super().__init__()
        self.basic_db_name = f'{self.project_path}/Warehouse/BasicDataBase.db'
        self.start_report = start_report
        self.g_today, self.j_today = self.today_date_as_string()

    def selected_date(self):
        conn_basic = sqlite3.connect(self.basic_db_name)
        date_df = self.load_table_as_dataframe("DateTbl", conn_basic,
                                                 "GDate")
        conn_basic.close()

        # تعریف شرایط فیلترینگ به صورت یک لیست از دیکشنری‌ها
        filter_conditions = [
            {'column_name': 'JDate', 'value': self.start_report, 'operator': '>='},
            {'column_name': 'JDate', 'value': self.j_today, 'operator': '<='},
        ]
        # فیلتر کردن DataFrame با استفاده از شرایط فیلترینگ
        date_df = self.create_filter_dataframe(date_df, filter_conditions)
        date_list = list(date_df["JDate"])

        return date_list

    def fetch_data_yekan(self):
        self.enter_site_yekan()
        # به دلیل فولدر دانلود که نمی توان درست انتخابش کرد فایل های بخش ورود اطلاعات را زودتر دریافت می کنیم.
        # فایل هایی که از قسمت ورود اطلاعات سایت یکان دریافت می شوند
        self.get_request_issuance_cancellation_Yekan()
        self.get_issuance_cancellation_Yekan()

        # فایل هایی که از قسمت گزارش های سایت یکان دریافت می شوند
        self.get_dailyreport_yekan(self.selected_date())
        # self.get_buysell_Yekan('1402-07-01')

start_day_report = '1403-02-01'
# finish_day_report = '1402-09-23'
yekan = FetchYekanData(start_day_report)
yekan.fetch_data_yekan()







# helper = DataHelper()
# db_name = '/home/shakour/shakour/Programming/Codes/ProFinancialDss/main_create_database/BasicDataBase.db'
# conn = sqlite3.connect(db_name)
# date_df = helper.load_table_as_dataframe("DateTbl", conn,
#                                                           "GDate")
# conn.close()
#
# # تعریف شرایط فیلترینگ به صورت یک لیست از دیکشنری‌ها
# filter_conditions = [
#     {'column_name': 'JDate', 'value': start_day_report , 'operator': '>='},
#     {'column_name': 'JDate', 'value': finish_day_report, 'operator': '<='},
# ]
#
# # فیلتر کردن DataFrame با استفاده از شرایط فیلترینگ
# date_df = helper.create_filter_dataframe(date_df, filter_conditions)
# date_list = list(date_df["JDate"])
#
#
#
# # ساخت نمونه از کلاس GetYekanDataOperation
# yekan_data_scraper = GetYekanData()
#
# # ورود به سایت
# yekan_data_scraper.enter_site_yekan('shakour', '$HAKour@09124467903')
# # دانلود گزارش های روزانه
# yekan_data_scraper.get_dailyreport_yekan(date_list)


# start_date = '1400-01-05'
# end_date = '1400-01-10'
# yekan_data_scraper.get_buysellreportevents_Yekan(start_date, end_date)

