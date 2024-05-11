# developed by: Shakour Alishahi
# ======================================================================================================================
# ######################################################################################################################
# import external libraries
# ----------------------------------------------------------------------------------------------------------------------
import sqlite3

from Foundation.FilterFramesHelper import FilterFramesHelper
# ======================================================================================================================
# ######################################################################################################################
# import internal libraries
# ----------------------------------------------------------------------------------------------------------------------
from RawMaterials.data_base_obj import DataHelper

from Bulkheed.create_df_from_tables import IranMarketMakerTableFrameBuilder

from Bulkheed.iran_market_maker_database_opr import (
    MarketMakerAssetsRayanYekanTblCreator,
    MarketMakerBasicFundsInformationTblCreator,
    MarketMakerFundsFiscalYearYekanTblCreator,
    MarketMakerInvestorsYekanTblCreator,
    MarketMakerInvestorsFundsTblCreator,
    MarketMakerDailyYekanReportsTblCreator, MarketMakerAnnouncementsInformationTbl,
    FundsProcessedVfmCreator, PreprocessDailyYekanReportTblCreator, WorldDictTblCreator,
    MarketMakerHoldingsYekanTblCreator, FundsInvestorsTblCreator, FundsInvestorsProcessedVfmCreator,
    InvestorsProcessedVfmCreator, HoldingsProcessorVfmCreator, GeneralProcessorVfmCreator
)




# ======================================================================================================================
# ######################################################################################################################
# Database call
# ----------------------------------------------------------------------------------------------------------------------

# ======================================================================================================================
class MarketMakerDBManager:
    def __init__(self, db_name):
        self.market_maker_db = db_name

    @DataHelper.calculate_execution_time
    def create_market_maker_assets_yekan_table(self):
        creator = MarketMakerAssetsRayanYekanTblCreator(self.market_maker_db)
        creator.create_MarketMakerAssetsRayanYekanTbl()
        print(f"MarketMakerAssetsYekanTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_market_maker_basic_funds_information_table(self):
        creator = MarketMakerBasicFundsInformationTblCreator(self.market_maker_db)
        creator.create_MarketMakerBasicFundsInformationTbl()
        print(f"MarketMakerBasicFundsInformationTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_market_maker_funds_fiscal_year_yekan_table(self):
        creator = MarketMakerFundsFiscalYearYekanTblCreator(self.market_maker_db)
        creator.create_MarketMakerFundsFiscalYearYekanTbl()
        print(f"MarketMakerFundsFiscalYearYekanTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_market_maker_announcements_information_table(self):
        creator = MarketMakerAnnouncementsInformationTbl(self.market_maker_db)
        creator.create_MarketMakerAnnouncementsInformationTbl()
        print(f"MarketMakerAnnouncementsInformationTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_market_maker_investors_yekan_table(self):
        creator = MarketMakerInvestorsYekanTblCreator(self.market_maker_db)
        creator.create_MarketMakerInvestorsYekanTbl()
        print(f"MarketMakerInvestorsYekanTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_market_maker_investors_funds_table(self):
        creator = MarketMakerInvestorsFundsTblCreator(self.market_maker_db)
        creator.create_MarketMakerInvestorsFundsTbl()
        print(f"MarketMakerInvestorsFundsTb created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_market_maker_holdings_table(self):
        creator = MarketMakerHoldingsYekanTblCreator(self.market_maker_db)
        creator.create_MarketMakerHoldingsTbl()
        print(f"MarketMakerHoldingsTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_market_maker_daily_reports_table(self, start_report, end_report):
        creator = MarketMakerDailyYekanReportsTblCreator(self.market_maker_db, start_report, end_report)
        creator.create_MarketMakerDailyYekanReportsHelperTbl()
        print(f"MarketMakerDailyYekanReportsHelperTbl created and data inserted successfully in {self.market_maker_db}.")
        creator.create_MarketMakerDailyYekanReportsTbl()
        print(f"MarketMakerDailyYekanReportsTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_PreprocessDailyYekanReportTbl(self):
        helper = FilterFramesHelper()
        symbols = helper.fetch_market_maker_symbols()
        creator = PreprocessDailyYekanReportTblCreator(self.market_maker_db, symbols)
        creator.create_PreprocessDailyYekanReportTbl()

    @DataHelper.calculate_execution_time
    def create_FundsProcessedVfm(self, j_date):
        helper = FilterFramesHelper()
        symbols = helper.fetch_market_maker_symbols()
        creator = FundsProcessedVfmCreator(self.market_maker_db, symbols, j_date)
        creator.create_funds_processed_vfm()

    @DataHelper.calculate_execution_time
    def backup_FundsProcessedVfm(self):
        create_tfm = IranMarketMakerTableFrameBuilder()
        FundsProcessedTfm = create_tfm.build_FundsProcessedTfm()
        helper = DataHelper()
        FundsProcessedTfm = helper.move_column_to_first(FundsProcessedTfm, "TimeFrameReportID")

        selected_columns = ['AdjBreakEvenPoint']

        FundsProcessedTfm[selected_columns] = FundsProcessedTfm.groupby('ShortName')[selected_columns].transform(
            lambda group: group.ffill().bfill())

        if 'NAVReturn' in FundsProcessedTfm.columns:
            FundsProcessedTfm = FundsProcessedTfm.drop('NAVReturn', axis=1)

        filter_helper = FilterFramesHelper()
        WholeJDateDailyYekanReportHelperTfm = filter_helper.build_filter_by_column_value_df(FundsProcessedTfm, 'TimeFrame', 'JDate')

        conn = sqlite3.connect(self.market_maker_db)
        FundsProcessedTfm.to_sql("BackUpFundsProcessedVfm", conn, index=False, if_exists='replace')
        WholeJDateDailyYekanReportHelperTfm.to_sql("BackUpWholeJDateDailyYekanReportHelperTfm", conn, index=False,
                                                  if_exists='replace')

        WholeJWeekYekanReportHelperTfm = filter_helper.build_filter_by_column_value_df(FundsProcessedTfm, 'TimeFrame', 'JWeekNumber')

        WholeJWeekYekanReportHelperTfm.to_sql("BackUpWholeJWeekYekanReportHelperTfm", conn, index=False,
                                                  if_exists='replace')

        conn.close()

    @DataHelper.calculate_execution_time
    def create_WholeJDateDailyYekanReportHelperVfm(self, j_date):
        helper = FilterFramesHelper()
        symbols = helper.fetch_market_maker_symbols()
        creator = FundsProcessedVfmCreator(self.market_maker_db, symbols, j_date)
        creator.create_whole_j_date_daily_yekan_report_helperTfm()

    @DataHelper.calculate_execution_time
    def create_WordDictTbl(self):
        creator = WorldDictTblCreator(self.market_maker_db)
        creator.create_word_dict_table()
        print(f"WordDictTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_RawMarketMakerIssuanceCancellationTbl(self):
        creator = FundsInvestorsTblCreator(self.market_maker_db)
        creator.create_raw_market_maker_issuance_cancellation_tbl()
        print(f"RawMarketMakerIssuanceCancellationTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_FundsInvestorsProcessedHelperTbl(self):
        creator = FundsInvestorsProcessedVfmCreator(self.market_maker_db)
        creator.create_funds_investors_helper_view_frame()
        print(f"FundsInvestorsProcessedHelperTbl created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_FundsInvestorsProcessedVfm(self):
        creator = FundsInvestorsProcessedVfmCreator(self.market_maker_db)
        creator.create_funds_investors_processed_view_frame()
        print(f"FundsInvestorsProcessedVfm created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_InvestorsProcessedVfm(self):
        creator = InvestorsProcessedVfmCreator(self.market_maker_db)
        creator.create_investors_processed_view_frame()
        print(f"InvestorsProcessedVfm created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_HoldingsProcessedVfm(self):
        creator = HoldingsProcessorVfmCreator(self.market_maker_db)
        creator.create_holdings_processed_view_frame()
        print(f"HoldingsProcessedVfm created and data inserted successfully in {self.market_maker_db}.")

    @DataHelper.calculate_execution_time
    def create_GeneralProcessorVfm(self):
        creator = GeneralProcessorVfmCreator(self.market_maker_db)
        creator.create_general_processed_view_frame()
        print(f"GeneralProcessedVfm created and data inserted successfully in {self.market_maker_db}.")



# Todo AnnouncementsInformation
import concurrent.futures

def manage_threads_start(which_op):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(which_op)


# Usage
project_path = "/home/shakour/shakour/Programming/Codes/GitStudy/MarketMakerReporter"
db_manager = MarketMakerDBManager(f"{project_path}/Warehouse/IranMarketMaker.db")

db_manager.create_market_maker_assets_yekan_table()
db_manager.create_market_maker_basic_funds_information_table()
db_manager.create_market_maker_funds_fiscal_year_yekan_table()
db_manager.create_market_maker_announcements_information_table()
db_manager.create_market_maker_investors_yekan_table()
db_manager.create_market_maker_holdings_table()
db_manager.create_market_maker_investors_funds_table()

db_manager.create_WordDictTbl()

db_manager.create_market_maker_daily_reports_table('1397-05-08', '1403-02-21')
db_manager.create_PreprocessDailyYekanReportTbl()
db_manager.create_WholeJDateDailyYekanReportHelperVfm("1403-02-21")
db_manager.create_FundsProcessedVfm("1403-02-21")

db_manager.create_RawMarketMakerIssuanceCancellationTbl()
db_manager.create_FundsInvestorsProcessedHelperTbl()
db_manager.create_FundsInvestorsProcessedVfm()
db_manager.create_InvestorsProcessedVfm()
db_manager.create_HoldingsProcessedVfm()
db_manager.create_GeneralProcessorVfm()


# for tableau

# class DataForTableau(IranMarketMakerTableFrameBuilder):
#     def __init__(self):
#         super().__init__()
#
#     @DataHelper.calculate_execution_time
#     def create_funds_tableau_frame(self):
#         FundsProcessedTfm = self.build_FundsProcessedTfm()
#         funds_tableau_frame = FundsProcessedTfm.copy()
#         word_dict_df = self.build_WordDictTfm()
#
#         selected_columns = FundsProcessedTfm.columns
#
#         name_dict = word_dict_df[word_dict_df['SystemWord'].isin(selected_columns)].set_index('SystemWord')[
#             'PersianWord'].to_dict()
#
#         funds_tableau_frame.rename(columns=name_dict, inplace=True)
#
#         funds_tableau_frame["نام هفته"] = funds_tableau_frame["شماره هفته"]
#
#         columns_to_translate = ['نیم سال', 'فصل', 'ماه', 'نام هفته', 'روز هفته', 'چارچوب زمانی', 'شی تاریخ شمسی'
#             , 'نوع اتفاق']
#
#         funds_tableau_frame = self.translate_values(funds_tableau_frame, columns_to_translate, word_dict_df)
#
#         # ایجاد فایل Excel با نام 'FundsProcessor'
#         funds_tableau_frame.to_excel("main_create_database/FundsTableauFrame.xlsx", index=False)
#         return funds_tableau_frame
#
#     @DataHelper.calculate_execution_time
#     def create_funds_investors_tableau_frame(self):
#         FundaInvestorsProcessedTfm = self.build_FundsInvestorsProcessedTfm()
#         funds_investors_tableau_frame = FundaInvestorsProcessedTfm.copy()
#         word_dict_df = self.build_WordDictTfm()
#
#         selected_columns = funds_investors_tableau_frame.columns
#
#         name_dict = word_dict_df[word_dict_df['SystemWord'].isin(selected_columns)].set_index('SystemWord')[
#             'PersianWord'].to_dict()
#
#         funds_investors_tableau_frame.rename(columns=name_dict, inplace=True)
#
#         columns_to_translate = ['نیم سال', 'فصل', 'ماه', 'نام هفته', 'روز هفته', 'چارچوب زمانی', 'شی تاریخ شمسی'
#             , 'نوع اتفاق']
#
#         funds_investors_tableau_frame = self.translate_values(funds_investors_tableau_frame, columns_to_translate, word_dict_df)
#
#         # ایجاد فایل Excel با نام 'FundsProcessor'
#         funds_investors_tableau_frame.to_excel("main_create_database/FundsInvestorsTableauFrame.xlsx", index=False)
#         return funds_investors_tableau_frame
#
#     @DataHelper.calculate_execution_time
#     def create_investors_tableau_frame(self):
#         InvestorsProcessedTfm = self.build_InvestorsProcessedTfm()
#         investors_tableau_frame = InvestorsProcessedTfm.copy()
#         word_dict_df = self.build_WordDictTfm()
#
#         selected_columns = investors_tableau_frame.columns
#
#         name_dict = word_dict_df[word_dict_df['SystemWord'].isin(selected_columns)].set_index('SystemWord')[
#             'PersianWord'].to_dict()
#
#         investors_tableau_frame.rename(columns=name_dict, inplace=True)
#
#         columns_to_translate = ['نیم سال', 'فصل', 'ماه', 'نام هفته', 'روز هفته', 'چارچوب زمانی', 'شی تاریخ شمسی'
#             , 'نوع اتفاق']
#
#         investors_tableau_frame = self.translate_values(investors_tableau_frame, columns_to_translate,
#                                                         word_dict_df)
#
#         # ایجاد فایل Excel با نام 'FundsProcessor'
#         investors_tableau_frame.to_excel("main_create_database/InvestorsTableauFrame.xlsx", index=False)
#         return investors_tableau_frame
#
#     @DataHelper.calculate_execution_time
#     def create_holdings_tableau_frame(self):
#         HoldingsProcessedTfm = self.build_HoldingsProcessedTfm()
#         holdings_tableau_frame = HoldingsProcessedTfm.copy()
#         word_dict_df = self.build_WordDictTfm()
#
#         selected_columns = holdings_tableau_frame.columns
#
#         name_dict = word_dict_df[word_dict_df['SystemWord'].isin(selected_columns)].set_index('SystemWord')[
#             'PersianWord'].to_dict()
#
#         holdings_tableau_frame.rename(columns=name_dict, inplace=True)
#
#         columns_to_translate = ['نیم سال', 'فصل', 'ماه', 'نام هفته', 'روز هفته', 'چارچوب زمانی', 'شی تاریخ شمسی'
#             , 'نوع اتفاق']
#
#         holdings_tableau_frame = self.translate_values(holdings_tableau_frame, columns_to_translate,
#                                                        word_dict_df)
#
#         # ایجاد فایل Excel با نام 'FundsProcessor'
#         holdings_tableau_frame.to_excel("main_create_database/HoldingsTableauFrame.xlsx", index=False)
#         return holdings_tableau_frame
#
#     @DataHelper.calculate_execution_time
#     def create_general_tableau_frame(self):
#         GeneralProcessedTfm = self.build_GeneralProcessedTfm()
#         general_tableau_frame = GeneralProcessedTfm.copy()
#         word_dict_df = self.build_WordDictTfm()
#
#         selected_columns = general_tableau_frame.columns
#
#         name_dict = word_dict_df[word_dict_df['SystemWord'].isin(selected_columns)].set_index('SystemWord')[
#             'PersianWord'].to_dict()
#
#         general_tableau_frame.rename(columns=name_dict, inplace=True)
#
#         columns_to_translate = ['نیم سال', 'فصل', 'ماه', 'نام هفته', 'روز هفته', 'چارچوب زمانی', 'شی تاریخ شمسی'
#             , 'نوع اتفاق']
#
#         general_tableau_frame = self.translate_values(general_tableau_frame, columns_to_translate,
#                                                       word_dict_df)
#
#         # ایجاد فایل Excel با نام 'FundsProcessor'
#         general_tableau_frame.to_excel("main_create_database/GeneralTableauFrame.xlsx", index=False)
#         return general_tableau_frame


# tableau = DataForTableau()
# tableau.create_funds_tableau_frame()
# tableau.create_investors_tableau_frame()
# tableau.create_funds_investors_tableau_frame()
# tableau.create_holdings_tableau_frame()
# tableau.create_general_tableau_frame()







