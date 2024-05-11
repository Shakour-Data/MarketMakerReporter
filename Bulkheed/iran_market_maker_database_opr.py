# developed by: Shakour Alishahi
# ======================================================================================================================
# ######################################################################################################################
# import external libraries
# ----------------------------------------------------------------------------------------------------------------------
import pandas as pd
from itertools import product
import numpy as np
import sqlite3

# ======================================================================================================================
# ######################################################################################################################
# import internal libraries
# ----------------------------------------------------------------------------------------------------------------------

from RawMaterials.data_base_obj import DataHelper

from Materials.create_df_from_tables import IranMarketMakerTableFrameBuilder

from Foundation.market_maker_tables_preprocessor import DailyYekanReportPreprocessor, FundsProcessor, \
    FundsInvestorsProcessor, InvestorsProcessor, HoldingsProcessor, GeneralProcessor


# ======================================================================================================================
# ######################################################################################################################
# Database call
# ----------------------------------------------------------------------------------------------------------------------


# ======================================================================================================================
# ######################################################################################################################
# Table: Create MarketMakerAssetsYekanTbl and insert data to it  -> Inheritance from class DataHelper
# DataBase: IranMarketMaker.db
# ----------------------------------------------------------------------------------------------------------------------
class MarketMakerAssetsRayanYekanTblCreator(DataHelper):

    """
    Create the 'MarketMakerAssetsRayanYekanTbl' table if it doesn't exist.

    """

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_MarketMakerAssetsRayanYekanTbl(self):
        excel_file = f"{self.project_path}/Mines/BasicMarketMakerFundsInformation.xlsx"
        sheet_name = "AssetsRayanYekan"
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        df = df.drop_duplicates(subset=["IranCompanyCode12"])
        conn = sqlite3.connect(self.db_name)

        dtyp = {
            "IranCompanyCode12": "TEXT PRIMARY KEY",
            "AssetNameYekan": "TEXT",
            "AssetSymbolYekan": "TEXT",
            "AssetTypeYekan": "TEXT",
            "AssetNameRayan": "TEXT" ,
            "AssetSymbolRayan": "TEXT"
        }

        # IranSymbol / Symbol
        df.to_sql("MarketMakerAssetsRayanYekanTbl", conn, index=False, if_exists='replace', dtype=dtyp)

# ######################################################################################################################
# Table: Create MarketMakerBasicFundsInformationTbl and insert data to it  -> Inheritance from class DataHelper
# DataBase: IranMarketMaker.db
# ----------------------------------------------------------------------------------------------------------------------
class MarketMakerBasicFundsInformationTblCreator(DataHelper):

    """
    Create the 'MarketMakerBasicFundsInformationTbl' table if it doesn't exist.
    """

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_MarketMakerBasicFundsInformationTbl(self):
        excel_file = f"{self.project_path}/Mines/BasicMarketMakerFundsInformation.xlsx"
        sheet_name = "BasicFundsInformation"
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        df = df.drop_duplicates(subset=["IranCompanyCode12"])
        conn = sqlite3.connect(self.db_name)

        dtyp = {
            "MarketMakerFundID": "INTEGER PRIMARY KEY",
            "IranCompanyCode12": "TEXT",
            "FundMarketMakerName": "TEXT",
            "SymbolFundYekan": "TEXT",
        }

        df.to_sql("MarketMakerBasicFundsInformationTbl", conn, index=False, if_exists='replace', dtype=dtyp)
# ======================================================================================================================
# ######################################################################################################################
# Table: Create MarketMakerBasicFundsFiscalYearYekanTbl and insert data to it  -> Inheritance from class DataHelper
# DataBase: IranMarketMaker.db
# ----------------------------------------------------------------------------------------------------------------------

class MarketMakerFundsFiscalYearYekanTblCreator(DataHelper):

    """
    Create the 'MarketMakerBasicFundsFiscalYearYekanTbl' table if it doesn't exist.
    """

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_MarketMakerFundsFiscalYearYekanTbl(self):
        excel_file = f"{self.project_path}/Mines/BasicMarketMakerFundsInformation.xlsx"
        sheet_name = "FundsFiscalYearYekan"
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        df = df.drop_duplicates(subset=["FundFiscalMarketMakerYekan"])
        conn = sqlite3.connect(self.db_name)

        dtyp = {
            "FundFiscalYearID": "INTEGER PRIMARY KEY",
            "IranCompanyCode12": "TEXT",
            "MarketMakerFundID": "INTEGER",
            "SymbolFundYekan": "TEXT",
            "FundFiscalMarketMakerYekan": "TEXT"
        }

        def split_text(text_):
            split_FundFiscalMarketMakerYekan = text_.split('(')
            split_name = split_FundFiscalMarketMakerYekan[0]  # اینجا تاریخ قرار می‌گیرد
            return split_name

        df["MarketMakerFundName"] = df[
            "FundFiscalMarketMakerYekan"].apply(split_text)

        df.to_sql("MarketMakerFundsFiscalYearYekanTbl", conn, index=False, if_exists='replace', dtype=dtyp)

# ######################################################################################################################
# Table: Create MarketMakerInvestorsTbl and insert data to it  -> Inheritance from class DataHelper
# DataBase: IranMarketMaker.db
# ----------------------------------------------------------------------------------------------------------------------

class MarketMakerInvestorsYekanTblCreator(DataHelper):
    """
    Create the 'MarketMakerInvestorsYekanTbl' table if it doesn't exist.
    """
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_MarketMakerInvestorsYekanTbl(self):
        excel_file = f"{self.project_path}/Mines/BasicMarketMakerFundsInformation.xlsx"
        sheet_name = "InvestorsYekan"
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        df = df.drop_duplicates(subset=["InvestorName"])
        conn = sqlite3.connect(self.db_name)

        dtyp = {

            "InvestorID": "INTEGER",
            "InvestorName": "TEXT",
            "NationalCode_UniversalCode": "TEXT PRIMARY KEY"

        }

        df.to_sql("MarketMakerInvestorsYekanTbl", conn, index=False, if_exists='replace', dtype=dtyp)

# ======================================================================================================================
# ######################################################################################################################
# Table: Create MarketMakerInvestorsTbl and insert data to it  -> Inheritance from class DataHelper
# DataBase: IranMarketMaker.db
# ----------------------------------------------------------------------------------------------------------------------
class MarketMakerAnnouncementsInformationTbl(DataHelper):

    """
    Create the 'MarketMakerBasicFundsFiscalYearYekanTbl' table if it doesn't exist.
    """

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_MarketMakerAnnouncementsInformationTbl(self):
        excel_file = f"{self.project_path}/Mines/BasicMarketMakerFundsInformation.xlsx"
        sheet_name = "AnnouncementInformation"
        df_announcements = pd.read_excel(excel_file, sheet_name=sheet_name)

        conn = sqlite3.connect(self.db_name)

        df_announcements['AnnouncementID'] = df_announcements['MarketMakerFundID'].astype(str) + '.0' + '-' + df_announcements['AnnouncementEffectiveJDate'].astype(str)
        df_announcements = df_announcements.drop_duplicates(subset=["AnnouncementID"])

        basic_fund_df = self.load_table_as_dataframe("MarketMakerBasicFundsInformationTbl", conn,
                                                 "IranCompanyCode12")

        self.mapping_columns(df_announcements, basic_fund_df, "SymbolFundYekan", "IranCompanyCode12",
                                         drop_pivot_column=False)

        dtyp = {
            "AnnouncementID": "TEXT PRIMARY KEY",
            "AnnouncementJDate": "TEXT",
            "AnnouncementType": "TEXT",
            "MarketMakerFundID": "INTEGER",
            "SymbolFundYekan": "TEXT",
            "AnnouncementEffectiveJDate": "TEXT",
            "StartMarketMakingJDate": "TEXT",
            "FinishMarketMakingJDate": "TEXT",
            "Commitment": "INTEGER",
            "CumulativeOrderVolume": "TEXT",
            "QuoteDomain": "REAL",
            "StockVolatilityRange": "INTEGER",
            "IranCompanyCode12": "TEXT"
        }

        df_announcements.to_sql("MarketMakerAnnouncementsInformationTbl", conn, index=False, if_exists='replace', dtype=dtyp)

        conn.close()

# ######################################################################################################################
# Table: Create MarketMakerHoldingsTbl and insert data to it  -> Inheritance from class DataHelper
# DataBase: IranMarketMaker.db
# ----------------------------------------------------------------------------------------------------------------------

class MarketMakerHoldingsYekanTblCreator(DataHelper):
    """
    Create the 'MarketMakerHoldingsYekanTbl' table if it doesn't exist.
    """
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_MarketMakerHoldingsTbl(self):
        excel_file = f"{self.project_path}/Mines/BasicMarketMakerFundsInformation.xlsx"
        sheet_name = "Holdings"
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        df = df.drop_duplicates(subset=["HoldingName"])
        conn = sqlite3.connect(self.db_name)

        dtyp = {

            "HoldingID": "INTEGER PRIMARY KEY",
            "HoldingName": "TEXT"

        }

        df.to_sql("MarketMakerHoldingsTbl", conn, index=False, if_exists='replace', dtype=dtyp)

# ======================================================================================================================
# LiquidityRatioState

# ######################################################################################################################
# Table: Create MarketMakerInvestorsFundsTbl and insert data to it  -> Inheritance from class DataHelper
# DataBase: IranMarketMaker.db
# ----------------------------------------------------------------------------------------------------------------------

class MarketMakerInvestorsFundsTblCreator(DataHelper):

    def __init__(self,db_name):
        super().__init__()
        self.db_name = db_name

    def create_MarketMakerInvestorsFundsTbl(self):
        conn = sqlite3.connect(self.db_name)

        # Read data from MarketMakerInvestorsFundsTbl table
        df_investors = self.load_table_as_dataframe("MarketMakerInvestorsYekanTbl", conn, "InvestorID")
        df_investors = df_investors.dropna()

        # Read data from MarketMakerBasicFundsInformationTbl table
        df_funds = self.load_table_as_dataframe("MarketMakerBasicFundsInformationTbl", conn, "MarketMakerFundID")
        df_funds = df_funds.dropna()

        # ایجاد تمام ترکیبات بین دو جدول با استفاده از تابع product
        combined_data = list(product(df_investors['InvestorName'],df_funds['SymbolFundYekan']))

        # ساخت DataFrame از تمام ترکیبات
        combined_df = pd.DataFrame(combined_data, columns=['InvestorName', 'SymbolFundYekan'])


        # pivot tables
        self.mapping_columns(combined_df, df_investors, "InvestorName", "InvestorID", drop_pivot_column=False)
        self.mapping_columns(combined_df, df_funds, "SymbolFundYekan", "MarketMakerFundID", drop_pivot_column=False)

        # ایجاد ستون جدید با ترکیب دو ستون موجود
        combined_df['InvestorFundsID'] =combined_df['InvestorID'].astype(str) + '-' + combined_df['MarketMakerFundID'].astype(str)

        dtyp = {
            "InvestorFundsID": "TEXT PRIMARY KEY",
            "InvestorID": "INTEGER",
            "MarketMakerFundID": "INTEGER",
            "InvestorName": "TEXT",
            "SymbolFundYekan": "TEXT"
        }

        new_column_order = ["InvestorFundsID", "InvestorID", "MarketMakerFundID", "InvestorName", "SymbolFundYekan"]

        combined_df = combined_df[new_column_order]

        combined_df.to_sql("MarketMakerInvestorsFundsTbl", conn, index=False, if_exists='replace', dtype=dtyp)

        conn.close()

# ######################################################################################################################
# Table: Create MarketMakerDailyYekanReportsTbl and insert data to it  -> Inheritance from class DataHelper
# DataBase: IranMarketMaker.db
# ----------------------------------------------------------------------------------------------------------------------

class MarketMakerDailyYekanReportsTblCreator(DataHelper):
    def __init__(self, db_name, start_report, end_report):
        super().__init__()
        self.db_name = db_name
        self.start_report = start_report
        self.end_report = end_report
        self.excel_path = f"{self.project_path}/Mines/YekanFiles/DailyReports"
        self.archive_path = f"{self.project_path}/Warehouse/YekanWarehouse/ArchiveYekanData/ArchiveDailyReports"

    def create_MarketMakerDailyYekanReportsHelperTbl(self):
        jdate_list = self.extraction_date_list(self.start_report, self.end_report)
        self.rename_date_excel_files(self.excel_path)
        conn = sqlite3.connect(self.db_name)

        dtyp = {
            "ReportID": "TEXT PRIMARY KEY",
            "JDate": "TEXT",
            "MarketMakerFundID": "INTEGER",
            "FundFiscalMarketMakerYekan": "TEXT",
            "SymbolFundYekan": "TEXT",
            "NumberOfStock": "INTEGER",
            "FinalPrice": "REAL",
            "BreakEvenPoint": "REAL",
            "NetSalesValue(FinalPrice)": "REAL",
            "BuyNumber": "REAL",
            "NetBuyAmount": "REAL",
            "SellNumber": "REAL",
            "NetSellAmount": "REAL",
            "Cash_CurrentBrokerage": "REAL",
            "Cash_BankDeposit": "REAL",
            "FundsFixedIncome": "REAL",
            "BondsFixedIncome": "REAL",
            "BoughtPower": "REAL",
            "NetCancellationAssets": "REAL",
            "TotalUnits": "REAL",
            "CancellationPrice": "REAL",
            "IssuePrice": "REAL",
            "FiscalYearReturn": "TEXT",
            "GDate": "TEXT",
            "Symbol": "TEXT",
            "PriceKey": "TEXT",
            "AnnouncementID": "TEXT",
            "HoldingID": "INTEGER"
        }
        for j_date in jdate_list:
            try:
                excel_file = f"{self.excel_path}/DailyReportYekan_{j_date}.xlsx"
                sheet_name = "Sheet1"
                df_report = pd.read_excel(excel_file, sheet_name=sheet_name)

                new_column_names = [
                    "ID", "FundFiscalMarketMakerYekan", "SymbolFundYekan", "NumberOfStock", "FinalPrice", "BreakEvenPoint",
                    "NetSalesValue(FinalPrice)", "BuyNumber", "NetBuyAmount", "SellNumber", "NetSellAmount", "Cash_CurrentBrokerage",
                    "Cash_BankDeposit", "FundsFixedIncome", "BondsFixedIncome", "BoughtPower", "NetCancellationAssets",
                    "TotalUnits", "CancellationPrice", "IssuePrice", "FiscalYearReturn"
                ]

                # تغییر نام تمام ستون‌ها
                df_report.columns = new_column_names

                # حذف ستون "Column_to_delete"
                df_report = df_report.drop("ID", axis=1)

                df_report["JDate"] = j_date

                # df_date = self.load_table_as_dataframe("DateTbl", conn_basic_database, "GDate")
                df_date = self.build_table_dataframe('BasicDataBase.db', 'DateTbl',
                                                     'GDate')

                df_report = self.mapping_columns(df_report, df_date, "JDate", "GDate", drop_pivot_column=False)

                # df_market_maker_basic = self.load_table_as_dataframe("MarketMakerBasicFundsInformationTbl", conn, "MarketMakerFundID")
                df_market_maker_basic = self.build_table_dataframe('IranMarketMaker.db',
                                                                                'MarketMakerBasicFundsInformationTbl',
                                                                                'MarketMakerFundID')
                df_market_maker_basic = df_market_maker_basic.dropna()

                df_report = self.mapping_columns(df_report, df_market_maker_basic, "SymbolFundYekan",
                                                 "MarketMakerFundID", drop_pivot_column=False)
                df_report["IranSymbol"] = df_report["SymbolFundYekan"]

                df_report['ReportID'] = df_report['MarketMakerFundID'].astype(str) + '-' + df_report['JDate'].astype(str)

                # TimeFrame
                df_report["TimeFrame"] = '1d'

                # symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn_iran_stock_database, "IranCompanyCode12")
                symbol_df = self.build_table_dataframe('IranStockDataBase.db', 'BasicIranSymbolsInformationTbl', 'IranCompanyCode12')

                df_report = self.mapping_columns(df_report, symbol_df, "IranSymbol", "IranCompanyCode12", drop_pivot_column=True)

                df_report = self.mapping_columns(df_report, symbol_df, "IranCompanyCode12","Symbol", drop_pivot_column=False)

                df_report["PriceKey"] = df_report["Symbol"] + "_" + df_report["GDate"] + "_" + df_report["TimeFrame"]

                AnnouncementsInformation_df = self.build_table_dataframe('IranMarketMaker.db',
                                                                                'MarketMakerAnnouncementsInformationTbl',
                                                                                'AnnouncementID')

                AnnouncementsInformation_df["ReportID_"] = AnnouncementsInformation_df["AnnouncementID"]
                df_report["ReportID_"] = df_report["ReportID"]
                df_report = self.mapping_columns(df_report, AnnouncementsInformation_df, "ReportID_", "AnnouncementID",
                                                 drop_pivot_column=True)

                df_report = self.mapping_columns(df_report, df_market_maker_basic, 'SymbolFundYekan',
                                                 'HoldingID', False)

                new_column_order = [
                    "ReportID", "JDate", "GDate", "MarketMakerFundID", "FundFiscalMarketMakerYekan", "SymbolFundYekan",
                    "Symbol", "NumberOfStock", "FinalPrice", "BreakEvenPoint", "NetSalesValue(FinalPrice)", "BuyNumber", "NetBuyAmount",
                    "SellNumber", "NetSellAmount", "Cash_CurrentBrokerage", "Cash_BankDeposit", "FundsFixedIncome",
                    "BondsFixedIncome", "BoughtPower", "NetCancellationAssets", "TotalUnits", "CancellationPrice",
                    "IssuePrice", "FiscalYearReturn", "PriceKey", "AnnouncementID", "HoldingID"
                ]
                df_report = df_report[new_column_order]
                df_report = df_report.dropna(subset=['MarketMakerFundID'])

                df_report.to_sql("MarketMakerDailyYekanReportsHelperTbl", conn, index=False, if_exists='append', dtype=dtyp)

                print(f"{j_date} was append")
            except:
                print("{j_date} is duplicate data")

        self.move_files(self.excel_path, self.archive_path, True)
        conn.close()

    def create_MarketMakerDailyYekanReportsTbl(self):
        conn = sqlite3.connect(self.db_name)
        tfm_builder = IranMarketMakerTableFrameBuilder()
        df_report_general = tfm_builder.build_MarketMakerDailyYekanReportsHelperTfm()

        dtyp = {
            "ReportID": "TEXT PRIMARY KEY",
            "JDate": "TEXT",
            "MarketMakerFundID": "INTEGER",
            "FundFiscalMarketMakerYekan": "TEXT",
            "SymbolFundYekan": "TEXT",
            "NumberOfStock": "INTEGER",
            "FinalPrice": "REAL",
            "BreakEvenPoint": "REAL",
            "NetSalesValue(FinalPrice)": "REAL",
            "BuyNumber": "REAL",
            "NetBuyAmount": "REAL",
            "SellNumber": "REAL",
            "NetSellAmount": "REAL",
            "Cash_CurrentBrokerage": "REAL",
            "Cash_BankDeposit": "REAL",
            "FundsFixedIncome": "REAL",
            "BondsFixedIncome": "REAL",
            "BoughtPower": "REAL",
            "NetCancellationAssets": "REAL",
            "TotalUnits": "REAL",
            "CancellationPrice": "REAL",
            "IssuePrice": "REAL",
            "FiscalYearReturn": "TEXT",
            "GDate": "TEXT",
            "Symbol": "TEXT",
            "PriceKey": "TEXT",
            "AnnouncementID": "TEXT",
            "HoldingID": "INTEGER"
        }

        df_report_general = df_report_general.sort_values(by='GDate', ascending=True)
        df_report_general = df_report_general.reset_index()
        df_report_general = df_report_general.drop(columns=["index"])
        print(df_report_general)

        # پر کردن مقادیر خالی ستون "AnnouncementID" فقط اگر شرط برقرار باشد
        AnnouncementsInformation_df = self.build_table_dataframe('IranMarketMaker.db',
                                                                        'MarketMakerAnnouncementsInformationTbl',
                                                                        'AnnouncementID')

        self.mapping_columns(df_report_general, AnnouncementsInformation_df, 'AnnouncementID',
                             'FinishMarketMakingJDate', False)

        # # پر کردن مقادیر خالی ستون "FinishMarketMakingJDate"

        df_report_general['FinishMarketMakingJDate'] = df_report_general.groupby('Symbol', group_keys=False)[
            'FinishMarketMakingJDate'].ffill()

        # دستیابی به مقدار‌ها با استفاده از یک شرط
        condition = df_report_general['JDate'] < df_report_general['FinishMarketMakingJDate']
        df_report_general.loc[condition, 'AnnouncementID'] = \
            df_report_general.loc[condition].groupby('Symbol', group_keys=False)['AnnouncementID'].ffill()

        df_report_general = df_report_general.drop(columns=['FinishMarketMakingJDate'])

        df_report_general.to_sql("MarketMakerDailyYekanReportsTbl", conn, index=False, if_exists='replace', dtype=dtyp)

        conn.close()


class PreprocessDailyYekanReportTblCreator(DailyYekanReportPreprocessor):
    def __init__(self, db_name, symbols_list):
        super().__init__()
        self.db_name = db_name

    def create_PreprocessDailyYekanReportTbl(self):
        conn = sqlite3.connect(self.db_name)
        preprocessed_daily_report_yekan_df = self.build_preprocessed_daily_report_yekan_df()
        preprocessed_daily_report_yekan_df.to_sql("PreprocessDailyYekanReportTbl", conn, index=False, if_exists='replace')
        conn.close()


class FundsProcessedVfmCreator(FundsProcessor):
    def __init__(self, db_name, symbols_list, j_date):
        super().__init__(symbols_list, j_date)
        self.db_name = db_name

    def create_funds_processed_vfm(self):
        conn = sqlite3.connect(self.db_name)
        creator = self.build_funds_processed_vfm()
        creator.to_sql("FundsProcessedVfm", conn, index=False, if_exists='replace')
        conn.close()

    def create_whole_j_date_daily_yekan_report_helperTfm(self):
        conn = sqlite3.connect(self.db_name)
        creator, week_view_frame = self.build_JDateDailyYekanReportHelperVfm()
        creator.to_sql("WholeJDateDailyYekanReportHelperTfm", conn, index=False, if_exists='replace')
        week_view_frame.to_sql("week_view_frame", conn, index=False, if_exists='replace')
        conn.close()


class WorldDictTblCreator(DataHelper):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_word_dict_table(self):
        conn = sqlite3.connect(self.db_name)
        word_dict = pd.read_excel(f"{self.project_path}/Mines/WordDictTbl.xlsx")
        word_dict.to_sql("WordDictTbl", conn, index=True, if_exists='replace', index_label='WordID')
        conn.close()
# ======================================================================================================================
# Table: Create MarketMakerIssuanceCancellationTbl and insert data to it
# DataBase: IranMarketMaker.db
# ----------------------------------------------------------------------------------------------------------------------
class FundsInvestorsTblCreator(FundsInvestorsProcessor):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name
        self.excel_path = f"{self.project_path}//home/shakour/shakour/Programming/Codes/GitStudy/MarketMakerReporter/Mines/YekanFiles/IssuanceCancellation"


    def create_raw_market_maker_issuance_cancellation_tbl(self):
        conn = sqlite3.connect(self.db_name)

        issuance_cancellation_excel_file = f"{self.excel_path}/IssuanceCancellation.xlsx"
        sheet_name = "AjaxList"
        df_issuance_cancellation = pd.read_excel(issuance_cancellation_excel_file, sheet_name=sheet_name)
        df_issuance_cancellation.drop(columns=['واریز از محل'], inplace=True)
        df_issuance_cancellation.reset_index(drop=True, inplace=True)

        issuance_cancellation_investment_excel_file = f"{self.excel_path}/InvestmentIssuanceCancellation.xlsx"
        sheet_name = "Sheet1"
        df_issuance_cancellation_investment = pd.read_excel(issuance_cancellation_investment_excel_file,
                                                            sheet_name=sheet_name)
        df_issuance_cancellation_investment.drop(columns=['Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15'], inplace=True)

        df_issuance_cancellation_investment.reset_index(drop=True, inplace=True)

        new_columns = {
            'ردیف': 'ID',
            'صندوق': 'FundFiscalMarketMakerYekan',
            'کد سهامداری': 'ShareholdingCode',
            'سرمایه‌گذار': 'InvestorName',
            'کد ملی / کد فراگیر': 'NationalCode_UniversalCode',
            'نوع عملیات': 'OperationType',
            'نوع واحد': 'UnitType',
            'تاریخ': 'DateTime',
            'تعداد': 'IssuedCancellationUnitsNumber',
            'قیمت': 'IssuedCancellationPrice',
            'مبلغ کل': 'Amount',
            'شماره رسید': 'ReceiptNumber',
            'وضعیت': 'situation'
        }

        # تغییر نام تمام ستون‌ها
        df_issuance_cancellation.rename(columns=new_columns, inplace=True)
        df_issuance_cancellation_investment.rename(columns=new_columns, inplace=True)

        result_issuance_cancellation = pd.concat([df_issuance_cancellation, df_issuance_cancellation_investment],
                                                 ignore_index=True)

        df_issuance_cancellation_investment["OperationType"].replace("واریز سرمایه", "صدور", inplace=True)

        result_issuance_cancellation.drop(columns=["ID"], inplace=True)

        self.convert_datetime_column(result_issuance_cancellation, "DateTime", "JDate",
                                     "Time", '/', '-',
                                     True, True)

        def split_text(text_):
            split_FundFiscalMarketMakerYekan = text_.split('(')
            split_name = split_FundFiscalMarketMakerYekan[0]  # اینجا تاریخ قرار می‌گیرد
            return split_name

        result_issuance_cancellation["JDate"] = result_issuance_cancellation["JDate"].str.replace("/", "-")

        result_issuance_cancellation["IssuedUnitsNumber"] = np.where(
            result_issuance_cancellation["OperationType"].isin(["صدور", "واریز سرمایه"]),
            result_issuance_cancellation["IssuedCancellationUnitsNumber"],
            0
        )

        result_issuance_cancellation["CancellationUnitsNumber"] = np.where(result_issuance_cancellation["OperationType"] == "ابطال",
                                                               result_issuance_cancellation["IssuedCancellationUnitsNumber"],
                                                               0)

        result_issuance_cancellation["IssuedAmount"] = np.where(
            result_issuance_cancellation["OperationType"].isin(["صدور", "واریز سرمایه"]),
            result_issuance_cancellation["Amount"],
            0
        )

        result_issuance_cancellation["CancellationAmount"] = np.where(result_issuance_cancellation["OperationType"] == "ابطال",
                                                               result_issuance_cancellation["Amount"],
                                                               0)

        result_issuance_cancellation.sort_values(by=['JDate'], inplace=True)

        result_issuance_cancellation.reset_index(drop=True, inplace=True)

        result_issuance_cancellation["MarketMakerFundName"] = result_issuance_cancellation[
            "FundFiscalMarketMakerYekan"].apply(split_text)

        date_tfm = self.build_DateTfm()
        self.mapping_columns(result_issuance_cancellation, date_tfm, "JDate", "GDate")

        # Replace 0 values in 'IssuedCancellationPrice' column with the integer result of 'Amount' divided by 'IssuedCancellationUnitsNumber'
        result_issuance_cancellation['IssuedCancellationPrice'] = np.where(
            result_issuance_cancellation['IssuedCancellationPrice'] == 0,
            (result_issuance_cancellation['Amount'] / result_issuance_cancellation[
                'IssuedCancellationUnitsNumber']).astype(int),
            result_issuance_cancellation['IssuedCancellationPrice']
        )

        dtyp = {
            "ID": "INTEGER PRIMARY KEY",
            "FundFiscalMarketMakerYekan": "TEXT",
            "ShareholdingCode": "TEXT",
            "InvestorName": "TEXT",
            "NationalCode_UniversalCode": "TEXT",
            "OperationType": "TEXT",
            "DepositPlace": "TEXT",
            "UnitType": "TEXT",
            "DateTime": "TEXT",
            "IssuedCancellationUnitsNumber": "INTEGER",
            "IssuedCancellationPrice": "INTEGER",
            "Amount": "INTEGER",
            "ReceiptNumber": "TEXT",
            "situation": "TEXT",
        }

        result_issuance_cancellation.to_sql("RawMarketMakerIssuanceCancellationTbl", conn, index=True,
                                            if_exists='replace', index_label="ID", dtype=dtyp)
        conn.close()


class FundsInvestorsProcessedVfmCreator(FundsInvestorsProcessor):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_funds_investors_helper_view_frame(self):
        conn = sqlite3.connect(self.db_name)
        funds_investors_helper_df = self.create_funds_investors_processed_helper_vfm()
        funds_investors_helper_df.to_sql("FundsInvestorsProcessedHelperVfm", conn, index=False,
                                         if_exists='replace')

        conn.close()

    def create_funds_investors_processed_view_frame(self):
        conn = sqlite3.connect(self.db_name)
        funds_investors_processed_df = self.create_FundsInvestorProcessed_df()
        funds_investors_processed_df.to_sql("FundsInvestorsProcessedVfm", conn, index=False,
                                         if_exists='replace')

        conn.close()

class InvestorsProcessedVfmCreator(InvestorsProcessor):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_investors_processed_view_frame(self):
        conn = sqlite3.connect(self.db_name)
        investors_processed_df = self.create_investors_process_vfm()
        investors_processed_df.to_sql("InvestorsProcessedVfm", conn, index=True,
                                      if_exists='replace', index_label="ID")

        conn.close()


class HoldingsProcessorVfmCreator(HoldingsProcessor):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_holdings_processed_view_frame(self):
        conn = sqlite3.connect(self.db_name)
        investors_processed_df = self.create_holdings_process_vfm()
        investors_processed_df.to_sql("HoldingsProcessedVfm", conn, index=True,
                                      if_exists='replace', index_label="ID")

        conn.close()

class GeneralProcessorVfmCreator(GeneralProcessor):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_general_processed_view_frame(self):
        conn = sqlite3.connect(self.db_name)
        general_processed_df = self.create_general_process_vfm()
        general_processed_df.to_sql("GeneralProcessedVfm", conn, index=True,
                                      if_exists='replace', index_label="ID")

        conn.close()















