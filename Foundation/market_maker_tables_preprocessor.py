# developed by: Shakour Alishahi
# ======================================================================================================================
# ######################################################################################################################
# import external libraries
# ----------------------------------------------------------------------------------------------------------------------

import pandas as pd
import numpy as np
from itertools import product


# ======================================================================================================================
# ######################################################################################################################
# import internal libraries
# ----------------------------------------------------------------------------------------------------------------------

from Bulkheed.portfolio_management_obj import PortfolioManagementCalculator
from Foundation.FilterFramesHelper import FilterFramesHelper

from RawMaterials.data_base_obj import DataHelper
from Materials.preprocessor_obj import DataPreprocessor
from Materials.create_df_from_tables import \
    IranMarketMakerTableFrameBuilder, BasicDataBaseTableFrame, IranStockTableFrameBuilder


# ======================================================================================================================
# ######################################################################################################################
# Database call
# ----------------------------------------------------------------------------------------------------------------------


# ======================================================================================================================
# ######################################################################################################################

class AnnouncementsInformationPreprocessor(IranMarketMakerTableFrameBuilder, BasicDataBaseTableFrame,
                                           IranStockTableFrameBuilder, DataPreprocessor):

    def __init__(self):
        super().__init__()

    def build_preprocessed_announcementsInformation_df(self):
        announcementsInformation_tfm = self.build_AnnouncementsInformationTfm()
        date_tfm = self.build_DateTfm()

        date_columns_dict = {'AnnouncementJDate': 'AnnouncementGDate',
                             'AnnouncementEffectiveJDate': 'AnnouncementEffectiveGDate',
                             'StartMarketMakingJDate': 'StartMarketMakingGDate',
                             'FinishMarketMakingJDate': 'FinishMarketMakingGDate'
                             }

        for j_date_column in date_columns_dict.keys():
            announcementsInformation_tfm['JDate'] = announcementsInformation_tfm[j_date_column]
            self.mapping_columns(announcementsInformation_tfm, date_tfm, 'JDate', 'GDate', drop_pivot_column=True)
            announcementsInformation_tfm[date_columns_dict[j_date_column]] = announcementsInformation_tfm['GDate']

        preprocessed_announcementsInformation_df = announcementsInformation_tfm
        # preprocessed_announcementsInformation_df = announcementsInformation_tfm.drop(columns='GDate')

        symbol_tfm = self.build_BasicIranSymbolsInformationTfm()

        self.mapping_columns(preprocessed_announcementsInformation_df, symbol_tfm, 'IranCompanyCode12',
                             'ShortName', drop_pivot_column=False)

        return preprocessed_announcementsInformation_df


class DailyYekanReportPreprocessor(FilterFramesHelper, IranMarketMakerTableFrameBuilder):

    def __init__(self):
        super().__init__()

        self.main_dataframe = self.build_MarketMakerDailyYekanReportsTfm()
        self.market_maker_symbols_set = None

    def add_basic_columns(self):
        symbol_tfm = self.build_BasicIranSymbolsInformationTfm()
        self.mapping_columns(self.main_dataframe, symbol_tfm, 'Symbol',
                             'ShortName', drop_pivot_column=False)
        self.mapping_columns(self.main_dataframe, symbol_tfm, 'Symbol',
                             'IranSymbol', drop_pivot_column=False)

        self.market_maker_symbols_set = list(set(self.main_dataframe["ShortName"]))

        announcements = AnnouncementsInformationPreprocessor()
        preprocessed_announcementsInformation_df = announcements.build_preprocessed_announcementsInformation_df()

        self.mapping_columns(self.main_dataframe, preprocessed_announcementsInformation_df, 'AnnouncementID',
                             'ContractNumber', drop_pivot_column=False)

        self.mapping_columns(self.main_dataframe, preprocessed_announcementsInformation_df, 'AnnouncementID',
                             'AnnouncementType', drop_pivot_column=False)

        self.mapping_columns(self.main_dataframe, preprocessed_announcementsInformation_df, 'AnnouncementID',
                             'AnnouncementEffectiveJDate', drop_pivot_column=False)

        self.mapping_columns(self.main_dataframe, preprocessed_announcementsInformation_df, 'AnnouncementID',
                             'FinishMarketMakingJDate', drop_pivot_column=False)

        self.mapping_columns(self.main_dataframe, preprocessed_announcementsInformation_df, 'AnnouncementID',
                             'Commitment', drop_pivot_column=False)

        self.mapping_columns(self.main_dataframe, preprocessed_announcementsInformation_df, 'AnnouncementID',
                             'CumulativeOrderVolume', drop_pivot_column=False)

        self.mapping_columns(self.main_dataframe, preprocessed_announcementsInformation_df, 'AnnouncementID',
                             'QuoteDomain', drop_pivot_column=False)

        holdings_tfm = self.build_MarketMakerHoldingsTfm()

        self.mapping_columns(self.main_dataframe, holdings_tfm, 'HoldingID',
                             'HoldingName', drop_pivot_column=True)

    def add_jalali_objects_columns(self):
        basic_database = BasicDataBaseTableFrame()
        date_tfm = basic_database.build_DateTfm()

        self.mapping_columns(self.main_dataframe, date_tfm, 'JDate', 'JYear')
        self.mapping_columns(self.main_dataframe, date_tfm, 'JDate', 'JHalfYear')
        self.mapping_columns(self.main_dataframe, date_tfm, 'JDate', 'JSeason')
        self.mapping_columns(self.main_dataframe, date_tfm, 'JDate', 'JMonthYear')
        self.mapping_columns(self.main_dataframe, date_tfm, 'JDate', 'JMonthNumber')
        self.mapping_columns(self.main_dataframe, date_tfm, 'JDate', 'JWeekNumber')
        self.mapping_columns(self.main_dataframe, date_tfm, 'JDate', 'JDayOfMonth')
        self.mapping_columns(self.main_dataframe, date_tfm, 'JDate', 'DayOfWeek')

    def add_derived_columns(self):
        self.main_dataframe["Cash"] = self.main_dataframe['Cash_CurrentBrokerage'] + self.main_dataframe[
            'Cash_BankDeposit']

        self.main_dataframe["FixedIncome"] = self.main_dataframe['FundsFixedIncome'] + self.main_dataframe[
            'BondsFixedIncome']
        self.main_dataframe["BuySellNumber"] = self.main_dataframe["BuyNumber"] + self.main_dataframe["SellNumber"]
        self.main_dataframe["NetBuySellAmount"] = self.main_dataframe["NetBuyAmount"] + self.main_dataframe[
            "NetSellAmount"]

        self.main_dataframe["CommitmentAmount"] = self.main_dataframe["Commitment"] * self.main_dataframe["FinalPrice"]

        self.main_dataframe["ToleratingSQWithBoughtPower"] = self.main_dataframe["BoughtPower"] / self.main_dataframe[
            "CommitmentAmount"]
        self.main_dataframe["ToleratingSQWithCash"] = self.main_dataframe["Cash"] / self.main_dataframe[
            "CommitmentAmount"]
        self.main_dataframe["ToleratingBQ"] = self.main_dataframe["NumberOfStock"] / self.main_dataframe["Commitment"]

        self.main_dataframe["AssetsValue"] = self.main_dataframe['NetSalesValue(FinalPrice)'] + \
                                             self.main_dataframe['Cash_CurrentBrokerage'] + self.main_dataframe[
                                                 'Cash_BankDeposit'] + \
                                             self.main_dataframe['FundsFixedIncome'] + self.main_dataframe[
                                                 'BondsFixedIncome']

        self.main_dataframe["NetAssetsValue"] = self.main_dataframe["TotalUnits"] * self.main_dataframe[
            "CancellationPrice"]
        self.main_dataframe["CreditValue"] = self.main_dataframe["NetAssetsValue"] - self.main_dataframe["AssetsValue"]

        # --------------------------------------------------------------------------------------------------------------

    def add_nav_columns(self):
        PreprocessedIranMarketPricesTfm = self.build_PreprocessedIranMarketPricesTfm()
        self.mapping_columns(self.main_dataframe, PreprocessedIranMarketPricesTfm, 'PriceKey', 'Close',
                             drop_pivot_column=False)
        self.mapping_columns(self.main_dataframe, PreprocessedIranMarketPricesTfm, 'PriceKey', 'AdjClose',
                             drop_pivot_column=False)
        self.mapping_columns(self.main_dataframe, PreprocessedIranMarketPricesTfm, 'PriceKey', 'AdjOpen',
                             drop_pivot_column=False)
        self.mapping_columns(self.main_dataframe, PreprocessedIranMarketPricesTfm, 'PriceKey', 'Volume',
                             drop_pivot_column=False)
        self.mapping_columns(self.main_dataframe, PreprocessedIranMarketPricesTfm, 'PriceKey', 'AdjVolume',
                             drop_pivot_column=False)
        self.mapping_columns(self.main_dataframe, PreprocessedIranMarketPricesTfm, 'PriceKey', 'TransactionValue',
                             drop_pivot_column=False)

        self.main_dataframe['AdjFactor'] = self.main_dataframe['AdjClose'] / self.main_dataframe['Close']
        self.main_dataframe['AdjFinal'] = self.main_dataframe['AdjFactor'] * self.main_dataframe['FinalPrice']
        self.main_dataframe['AdjBreakEvenPoint'] = self.main_dataframe['AdjFactor'] * self.main_dataframe['BreakEvenPoint']

        selected_columns = ['AdjOpen', 'Close', 'AdjClose', 'AdjFactor', 'FinalPrice', 'AdjFinal', 'AdjBreakEvenPoint']

        self.main_dataframe[selected_columns] = self.main_dataframe.groupby('ShortName')[selected_columns].ffill()

        columns_to_fill = {'Volume': 0, 'AdjVolume': 0, 'TransactionValue': 0}

        self.main_dataframe.fillna(value=columns_to_fill, inplace=True)

    def build_preprocessed_daily_report_yekan_df(self):
        self.add_basic_columns()
        self.add_jalali_objects_columns()
        self.add_nav_columns()
        self.add_derived_columns()

        preprocessed_daily_report_yekan_df = self.main_dataframe

        return preprocessed_daily_report_yekan_df


# reporter = DailyYekanReportPreprocessor()
# df = reporter.build_preprocessed_daily_report_yekan_df()
#
# df.to_excel("daily_report_yekan.xlsx")
# print(df)

class DailyYekanReportViewFrameCreator(DailyYekanReportPreprocessor, PortfolioManagementCalculator):
    def __init__(self, symbols_list, jalali_object):
        super().__init__()

        self.symbols = symbols_list
        self.main_vfm = self.build_preprocessed_daily_report_yekan_df()
        self.vfm = self.filter_by_short_names(self.main_vfm, self.symbols)
        self.vfm = self.vfm.reset_index()
        self.time_frame = jalali_object

    def cum_sum_columns_builder(self, column_name):
        mid_df = pd.DataFrame()
        mid_df["ReportID"] = self.vfm["ReportID"]
        if self.time_frame == "JDate":
            mid_df[f"CumSum{column_name}"] = self.vfm[f"{column_name}"]
        elif self.time_frame == "JYear":
            mid_df[f"CumSum{column_name}"] = self.vfm.groupby(["ShortName", "JYear"])[column_name].cumsum().fillna(method='ffill').to_frame(
                f"CumSum{column_name}")
        elif self.time_frame == "ContractNumber":
            mid_df[f"CumSum{column_name}"] = self.vfm.groupby(["ShortName", "ContractNumber"])[column_name].cumsum().fillna(
                method='ffill').to_frame(f"CumSum{column_name}")
        elif self.time_frame == "AnnouncementID":
            mid_df[f"CumSum{column_name}"] = self.vfm.groupby(["ShortName", "AnnouncementID"])[column_name].cumsum().fillna(
                method='ffill').to_frame(f"CumSum{column_name}")
        elif self.time_frame in ['JHalfYear', 'JSeason', 'JMonthYear', 'JWeekNumber']:
            mid_df[f"CumSum{column_name}"] = self.vfm.groupby(["ShortName", "JYear", self.time_frame])[column_name].cumsum().fillna(
                method='ffill').to_frame(f"CumSum{column_name}")

        return mid_df

    def build_cum_sum_columns(self):
        cols = ['BuyNumber', 'NetBuyAmount', 'SellNumber', 'NetSellAmount', 'Volume', 'AdjVolume', 'TransactionValue',
                'BuySellNumber', 'NetBuySellAmount']
        result_df = self.vfm.copy()  # ساخت یک کپی از self.vfm برای اعمال تغییرات

        for col in cols:
            # فراخوانی تابع cum_sum_columns_builder برای هر ستون
            mid_df = self.cum_sum_columns_builder(col)

            self.mapping_columns(result_df, mid_df, "ReportID", f"CumSum{col}")

        result_df = result_df.drop(columns=["index"])
        result_df["TimeFrame"] = self.time_frame

        return result_df

    @staticmethod
    def select_each_symbols_assets_columns():
        each_symbols_assets_columns = [
            "NetSalesValue(FinalPrice)",
            "Cash_CurrentBrokerage",
            "Cash_BankDeposit",
            "Cash",
            "FundsFixedIncome",
            "BondsFixedIncome",
            "FixedIncome",
            "BoughtPower",
            "NumberOfStock",
            "AssetsValue",
            "NetAssetsValue",
            "CreditValue",
            "ToleratingSQWithBoughtPower",
            "ToleratingSQWithCash",
            "ToleratingBQ",
        ]
        return each_symbols_assets_columns

    @staticmethod
    def select_each_symbols_nav_columns():
        each_symbols_nav_columns = [
            "TotalUnits",
            "AssetsValue",
            "CreditValue",
            "NetAssetsValue",
            "CancellationPrice",
            "IssuePrice",
            "BreakEvenPoint",
            "AdjOpen",
            "Close",
            "AdjClose",
            "FinalPrice",
            "AdjFinal",
        ]

        return each_symbols_nav_columns

    def select_each_symbols_transactions_columns(self, text):
        each_symbols_transactions_columns = list(self.vfm.columns[self.vfm.columns.str.contains(text)])
        return each_symbols_transactions_columns

    def select_jalali_object_01(self, short_name, j_date=None):
        # jalali_objects = ['JDate', 'JYear', 'JHalfYear', 'JSeason', 'JMonthYear', 'JWeekNumber', 'ContractNumber',
        #                   'AnnouncementID']
        dataframe = self.build_cum_sum_columns()
        df = self.filter_by_short_name(dataframe, short_name)
        jalali_values = list(set(self.vfm[self.time_frame]))

        dfs_list = []

        if self.time_frame != 'JDate':
            for jalali_value in jalali_values:
                try:
                    # Filter by the specific jalali value
                    filtered_df = self.filter_by_jalali_objects_date(df, self.time_frame, jalali_value)
                    # Filter by JYear and get the last row
                    selected_df = filtered_df.groupby("JYear").apply(lambda x: x.tail(1))
                    selected_df['JalaliObject'] = jalali_value
                    selected_df['YearObjectJalali'] = selected_df['JalaliObject'].astype(str) + ' ' + selected_df[
                        "JYear"].astype(str)
                except:
                    print(f'{jalali_value} is not a valid ')
                else:
                    dfs_list.append(selected_df)

            result = pd.concat(dfs_list, ignore_index=True)
            result.sort_values(by=['JDate'], inplace=True)
            result = result.drop_duplicates()
            result.reset_index(drop=True, inplace=True)
            result.dropna(subset=['JDate'], inplace=True)

        else:
            result = self.filter_by_jalali_date(dataframe, j_date)
            result["YearObjectJalali"] = result["JDate"]
            result["JalaliObject"] = result["JDate"]

        return result

    def select_jalali_object(self, short_name=None, j_date=None):
        if short_name is not None:
            result = self.select_jalali_object_01(short_name, j_date)
        else:
            results = []
            for short_name in self.symbols:
                result = self.select_jalali_object_01(short_name, j_date)
                results.append(result)
                print(short_name)

            result = pd.concat(results, ignore_index=True)
            result.sort_values(by=['JDate'], inplace=True)
            result = result.drop_duplicates()
            result.reset_index(drop=True, inplace=True)

        return result

    def build_costume_vfm(self, view_frame_type, short_name=None, j_date=None):
        result = self.select_jalali_object(short_name, j_date)

        if view_frame_type == "EachSymbolsAssets":
            if self.time_frame == "JYear":
                required_columns = ["JDate", "TimeFrame"] + self.select_each_symbols_assets_columns() + [
                    "ContractNumber", "AnnouncementID", "PriceKey"]
                result = result[required_columns]
            elif self.time_frame == "JDate":
                required_columns = ["JDate"] + self.select_each_symbols_assets_columns() + ["ContractNumber",
                                                                                            "AnnouncementID",
                                                                                            "PriceKey"]
                result = result[required_columns]
            elif self.time_frame in ['ContractNumber', 'AnnouncementID']:
                required_columns = ["JDate"] + self.select_each_symbols_assets_columns() + ["EventType",
                                                                                            "ContractNumber",
                                                                                            "AnnouncementID",
                                                                                            "PriceKey"]
                result = result[required_columns]
            else:
                required_columns = ["JDate", "JYear", "TimeFrame"] + self.select_each_symbols_assets_columns() + [
                    "ContractNumber", "AnnouncementID", "PriceKey"]
                result = result[required_columns]

        elif view_frame_type == "EachSymbolsNAV":
            if self.time_frame == "JYear":
                required_columns = ["JDate", "TimeFrame"] + self.select_each_symbols_nav_columns() + [
                    "ContractNumber", "AnnouncementID", "PriceKey"]
                result = result[required_columns]
            elif self.time_frame == "JDate":
                required_columns = ["JDate"] + self.select_each_symbols_nav_columns() + ["ContractNumber",
                                                                                         "AnnouncementID", "PriceKey"]
                result = result[required_columns]
            elif self.time_frame in ['ContractNumber', 'AnnouncementID']:
                required_columns = ["JDate"] + self.select_each_symbols_nav_columns() + ["EventType", "ContractNumber",
                                                                                         "AnnouncementID", "PriceKey"]
                result = result[required_columns]
            else:
                required_columns = ["JDate", "JYear", "TimeFrame"] + self.select_each_symbols_nav_columns() + [
                    "ContractNumber", "AnnouncementID", "PriceKey"]
                result = result[required_columns]


        return result

    # ------------------------------------------------------------------------------------------------------------------
# jalali_objects = ['JDate', 'JYear', 'JHalfYear', 'JSeason', 'JMonthYear', 'JWeekNumber', 'ContractNumber','AnnouncementID']
# symbol_list = ["Tehran Cement", "Kaveh Company"]
# re = DailyYekanReportViewFrameCreator(symbol_list, "JHalfYear")
# vfm = re.select_jalali_object()
# # vfm = re.build_costume_vfm("JYear", 'EachSymbolsTransactions', short_name='Kaveh Company', j_date=None)
# vfm.to_excel('vfm.xlsx')
# print(vfm.columns)
#
# import pandas as pd

class FundsProcessor(FilterFramesHelper, PortfolioManagementCalculator):
    """
    A class for creating view frames for whole general time frames in the IranMarketMaker.db database.
    """
    def __init__(self, symbols_list, j_date):
        """
        Initializes the FundsProcessor object.

        Args:
            symbols_list (list): A list of symbols.
            j_date (str): The Jalali date.
        """
        super().__init__()
        self.symbols_list = symbols_list
        self.j_date = j_date

    @DataHelper.calculate_execution_time
    def build_JDateDailyYekanReportHelperVfm(self):
        """
        Builds a view frame for the WholeTimeFramesDailyYekanReportVfm table in the IranMarketMaker.db database.

        The  `build_j_date_WholeTimeFramesDailyYekanReportTfm()`  function builds a view frame for the
         WholeTimeFramesDailyYekanReportVfm table in the IranMarketMaker.db database. It retrieves data from the
         WholeTimeFramesDailyYekanReportTfm table and filters it based on the 'TimeFrame' column value 'JDate'. If the table exists,
         it retrieves the last date from the filtered data and converts it to the corresponding Jalali date. Then, it
         builds a DateTfm table and filters it based on the start Jalali date and the current Jalali date. For each
         Jalali date, it selects the corresponding Jalali object from the DailyYekanReportViewFrameCreator and appends
         it to a list. Finally, it concatenates the view frames, sorts them by 'JDate', removes duplicates, and returns
         the resulting view frame.


        Returns:
            result_j_date_view_frame (pandas.DataFrame): The resulting view frame containing the data.
        """
        # Create a DailyYekanReportViewFrameCreator object
        report = DailyYekanReportViewFrameCreator(self.symbols_list, "JDate")

        # Initialize variables
        j_date_view_frames = []
        database = f"{self.project_path}/Warehouse/IranMarketMaker.db"
        table_name = "BackUpFundsProcessedVfm"

        # Check if the table exists
        if self.check_table_existence(database, table_name):
            # Retrieve data from WholeTimeFramesDailyYekanReportTfm table and filter by 'TimeFrame' column value 'JDate'
            WholeTimeFramesDailyYekanReportTfm = self.build_WholeJDateDailyYekanReportHelperTfm()
            whole_j_date_df = self.build_filter_by_column_value_df(WholeTimeFramesDailyYekanReportTfm, 'TimeFrame', 'JDate')

            # Get the last date from the filtered data and convert it to Jalali date
            start_georgian_date = self.get_last_date(whole_j_date_df, "GDate")
            print(start_georgian_date)
            start_jalali_date = self.gregorian_to_jalali(start_georgian_date)

        else:
            whole_j_date_df = pd.DataFrame()
            start_jalali_date = "1397-05-09"

        # Build DateTfm table and filter by Jalali dates
        date_tfm = self.build_DateTfm()
        date_tfm_filtered = self.filter_between_two_jalali_dates(date_tfm, start_jalali_date, self.j_today)
        j_date_list = list(date_tfm_filtered["JDate"])

        # Select Jalali objects for each Jalali date and append to the list
        for j_date in j_date_list:
            j_date_view_frame = report.select_jalali_object(short_name=None, j_date=j_date)
            j_date_view_frames.append(j_date_view_frame)
            print("JDate")
            print("j_date_list:", j_date)

        week_report = DailyYekanReportViewFrameCreator(self.symbols_list, "JWeekNumber")

        week_view_frame = week_report.select_jalali_object(short_name=None)

        week_view_frame.to_excel("week_view_frame.xlsx")

        # Concatenate the view frames, sort by 'JDate', remove duplicates, and reset the index
        result_update_j_date_view_frame = pd.concat(j_date_view_frames, ignore_index=True)
        result_j_date_view_frame = pd.concat([whole_j_date_df, result_update_j_date_view_frame], ignore_index=True)
        self.mapping_columns(result_j_date_view_frame, date_tfm, "JDate", "GDate")
        result_j_date_view_frame.sort_values(by=['JDate'], inplace=True)
        result_j_date_view_frame.drop_duplicates(subset=['ReportID', 'TimeFrame'], keep='first', inplace=True)
        result_j_date_view_frame["TimeFrameReportID"] = result_j_date_view_frame['ReportID'].astype(str) + '-' + result_j_date_view_frame[
            'TimeFrame'].astype(str)
        result_j_date_view_frame.reset_index(drop=True, inplace=True)
        return result_j_date_view_frame, week_view_frame

    @DataHelper.calculate_execution_time
    def build_funds_processed_vfm_helper(self):
        """
        Builds view frames for different time frames.

        Returns:
            result (pandas.DataFrame): The resulting view frame containing the data.
        """
        jalali_objects = ['JDate']

        view_frames = []
        for time_frame in jalali_objects:
            report = DailyYekanReportViewFrameCreator(self.symbols_list, time_frame)
            if time_frame not in ['JDate']:
                view_frame = report.select_jalali_object(short_name=None, j_date=self.j_date)
                view_frames.append(view_frame)
                print(time_frame)

            else:
                view_frame = self.build_WholeJDateDailyYekanReportHelperTfm()
                view_frames.append(view_frame)

        funds_processed_vfm_helper = pd.concat(view_frames, ignore_index=True)
        funds_processed_vfm_helper.sort_values(by=['JDate'], inplace=True)
        funds_processed_vfm_helper = funds_processed_vfm_helper.drop_duplicates()
        funds_processed_vfm_helper.reset_index(drop=True, inplace=True)
        return funds_processed_vfm_helper

    @DataHelper.calculate_execution_time
    def build_funds_processed_vfm(self):
        funds_processed_vfm_helper = self.build_funds_processed_vfm_helper()
        jalali_objects = ['JDate']

        result_dfs = []
        for short_name in self.symbols_list:
            print(short_name)
            try:
                report = DailyYekanReportViewFrameCreator([short_name], None)
                for time_frame in jalali_objects:
                    print(time_frame)
                    try:
                        result_df = self.filter_by_short_name(funds_processed_vfm_helper, short_name)
                        result_df = self.build_filter_by_column_value_df(result_df, "TimeFrame", time_frame)

                        result_df["PeriodNAVReturn"] = np.where(
                            result_df['CancellationPrice'].shift(1).notna(),
                            ((result_df['CancellationPrice'] - result_df['CancellationPrice'].shift(1)) / result_df[
                                'CancellationPrice'].shift(1)),
                            ((result_df['CancellationPrice'] - 1000000) / 1000000)
                        )

                        result_df["CumNAVReturn"] = (result_df['CancellationPrice'] - 1000000) / 1000000

                        result_df["AdjFinalReturn"] = np.where(
                            result_df['AdjFinal'].shift(1).notna(),
                            np.where(
                                np.isinf(((result_df['AdjFinal'] - result_df['AdjFinal'].shift(1)) / result_df[
                                    'AdjFinal'].shift(1))),
                                0,
                                ((result_df['AdjFinal'] - result_df['AdjFinal'].shift(1)) / result_df['AdjFinal'].shift(
                                    1))
                            ),
                            np.where(
                                np.isinf(((result_df['AdjFinal'] - result_df['AdjOpen']) / result_df['AdjOpen'])),
                                0,
                                ((result_df['AdjFinal'] - result_df['AdjOpen']) / result_df['AdjOpen'])
                            )
                        )

                        result_df["CumAdjFinalReturn"] = np.where(
                            result_df.index == 0,
                            0,
                            np.where(
                                np.isinf((result_df['AdjFinal'] - result_df['AdjFinal'].iloc[0]) /
                                         result_df['AdjFinal'].iloc[0]),
                                0,
                                ((result_df['AdjFinal'] - result_df['AdjFinal'].iloc[0]) / result_df['AdjFinal'].iloc[
                                    0])
                            )
                        )

                        result_dfs.append(result_df)

                    except:
                        print(f'{short_name}, in {time_frame} has not return')
            except:
                print(f'{short_name} is not valid')

        funds_processed_vfm = pd.concat(result_dfs, ignore_index=True)
        funds_processed_vfm.sort_values(by=['JDate'], inplace=True)
        funds_processed_vfm = funds_processed_vfm.drop_duplicates()
        funds_processed_vfm.reset_index(drop=True, inplace=True)
        funds_processed_vfm["TimeFrameReportID"] = funds_processed_vfm['ReportID'].astype(str) + '-' + funds_processed_vfm[
            'TimeFrame'].astype(str)
        funds_processed_vfm = self.move_column_to_first(funds_processed_vfm, "TimeFrameReportID")

        selected_columns = ['AdjOpen', 'Close', 'AdjClose', 'AdjFactor', 'FinalPrice', 'AdjFinal', 'AdjBreakEvenPoint']

        funds_processed_vfm[selected_columns] = funds_processed_vfm.groupby('ShortName')[selected_columns].ffill()

        return funds_processed_vfm

# helper = FilterFramesHelper()
# symbol_list = helper.fetch_market_maker_symbols()
# symbol_list = ['Sina Tile', 'Glass and Gas']
# print(symbol_list)
# re = FundsProcessor(symbol_list, "1402-10-29")
# funds_processed_vfm = re.build_funds_processed_vfm()
# funds_processed_vfm.to_excel("funds_processed_vfm.xlsx")
# vfm = re.build_general_holdings_vfm()
# # # vfm = re.build_costume_vfm("JYear", 'EachSymbolsTransactions', short_name='Kaveh Company', j_date=None)
# vfm.to_excel('general_holdings_vfm.xlsx')
# print(vfm)

class MarketMakerIssuanceCancellationPreprocessor(FilterFramesHelper, IranMarketMakerTableFrameBuilder):
    def __init__(self):
        super().__init__()

    @DataHelper.calculate_execution_time
    def InvestObjects_add_required_columns(self):

        RawMarketMakerIssuanceCancellationTfm = self.build_RawMarketMakerIssuanceCancellationTfm()

        InvestObjects_df = RawMarketMakerIssuanceCancellationTfm

        MarketMakerBasicFundsInformationTfm = self.build_MarketMakerBasicFundsInformationTfm()
        MarketMakerInvestorsYekanTfm = self.build_MarketMakerInvestorsYekanTfm()
        MarketMakerFundsFiscalYearYekanTfm = self.build_MarketMakerFundsFiscalYearYekanTfm()
        PreprocessDailyYekanReportTfm = self.build_PreprocessDailyYekanReportTfm()
        BasicIranSymbolsInformationTfm = self.build_BasicIranSymbolsInformationTfm()

        InvestObjects_df.sort_values(by=['JDate'], inplace=True)

        # --------------------------------------------------------------------------------------------------------------

        self.mapping_columns(InvestObjects_df, MarketMakerFundsFiscalYearYekanTfm, "MarketMakerFundName",
                             "IranCompanyCode12", False)
        self.mapping_columns(InvestObjects_df, MarketMakerBasicFundsInformationTfm, "IranCompanyCode12",
                             "MarketMakerFundID", False)\

        self.mapping_columns(InvestObjects_df, MarketMakerInvestorsYekanTfm, "NationalCode_UniversalCode",
                             "InvestorID", False)

        self.mapping_columns(InvestObjects_df, BasicIranSymbolsInformationTfm, "IranCompanyCode12",
                             "ShortName", False)

        InvestObjects_df['InvestorID'] = pd.to_numeric(InvestObjects_df['InvestorID'], errors='coerce').fillna(0).astype(int)

        self.mapping_columns(InvestObjects_df, MarketMakerFundsFiscalYearYekanTfm, "MarketMakerFundName",
                             "MarketMakerFundID", False)

        PreprocessDailyYekanReportTfm = self.build_PreprocessDailyYekanReportTfm()
        short_names = self.fetch_market_maker_symbols()

        # تابع برای تبدیل مقادیر
        def replace_values(value):
            if value < threshold:
                return threshold
            else:
                return value

        filtered_InvestObjects_dfs = []
        for short_name in short_names:
            filtered_PreprocessDailyYekanReportTfm = self.filter_by_short_name(PreprocessDailyYekanReportTfm,
                                                                               short_name)
            filtered_PreprocessDailyYekanReportTfm = filtered_PreprocessDailyYekanReportTfm.reset_index()

            if not filtered_PreprocessDailyYekanReportTfm.empty:
                first_value_A = filtered_PreprocessDailyYekanReportTfm.head(1)['JDate'].iloc[0]
                threshold = first_value_A  # عدد خاص

                filtered_InvestObjects_df = self.filter_by_short_name(InvestObjects_df, short_name)
                filtered_InvestObjects_df['JDate'] = filtered_InvestObjects_df['JDate'].apply(
                    lambda x: threshold if x < threshold else x)

                filtered_InvestObjects_dfs.append(filtered_InvestObjects_df)
            else:
                print(f"filtered_PreprocessDailyYekanReportTfm for {short_name} is empty or does not contain any rows.")

        # ادامه کار با filtered_InvestObjects_dfs...

        InvestObjects_df = pd.concat(filtered_InvestObjects_dfs, ignore_index=True)

        InvestObjects_df['ReportID'] = InvestObjects_df['MarketMakerFundID'].astype(str) + ".0" + '-' + InvestObjects_df['JDate'].astype(str)

        self.mapping_columns(InvestObjects_df, PreprocessDailyYekanReportTfm, "ReportID",
                             "TotalUnits", False)

        InvestObjects_df['InvestorID'] = InvestObjects_df['InvestorID'].astype(int)

        InvestObjects_df['InvestorFundsID'] = InvestObjects_df['InvestorID'].astype(str) + '-' + InvestObjects_df[
            'MarketMakerFundID'].astype(str)

        InvestObjects_df["FundCumSumIssuedUnitsNumber"] = InvestObjects_df.groupby(["IranCompanyCode12"])[
            'IssuedUnitsNumber'].cumsum()

        InvestObjects_df["FundCumSumCancellationUnitsNumber"] = InvestObjects_df.groupby(["IranCompanyCode12"])[
            'CancellationUnitsNumber'].cumsum()

        InvestObjects_df["FundCumSumIssuedAmount"] = InvestObjects_df.groupby(["IranCompanyCode12"])[
            'IssuedAmount'].cumsum()

        InvestObjects_df["FundCumSumCancellationAmount"] = InvestObjects_df.groupby(["IranCompanyCode12"])[
            'CancellationAmount'].cumsum()

        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        # Investors Columns
        InvestObjects_df["InvestorCumSumIssuedUnitsNumber"] = InvestObjects_df.groupby(["NationalCode_UniversalCode"])[
            'IssuedUnitsNumber'].cumsum()

        InvestObjects_df["InvestorCumSumCancellationUnitsNumber"] = InvestObjects_df.groupby(["NationalCode_UniversalCode"])[
            'CancellationUnitsNumber'].cumsum()


        # --------------------------------------------------------------------------------------------------------------
        # Funds Investors Columns
        InvestObjects_df["CumSumIssuedUnitsNumber"] = InvestObjects_df.groupby(["IranCompanyCode12", "NationalCode_UniversalCode"])[
            'IssuedUnitsNumber'].cumsum()

        InvestObjects_df["CumSumCancellationUnitsNumber"] = InvestObjects_df.groupby(["IranCompanyCode12", "NationalCode_UniversalCode"])[
            'CancellationUnitsNumber'].cumsum()

        InvestObjects_df["CumSumIssuedAmount"] = InvestObjects_df.groupby(["IranCompanyCode12", "NationalCode_UniversalCode"])[
            'IssuedAmount'].cumsum()

        InvestObjects_df["CumSumCancellationAmount"] = InvestObjects_df.groupby(["IranCompanyCode12", "NationalCode_UniversalCode"])[
            'CancellationAmount'].cumsum()

        # --------------------------------------------------------------------------------------------------------------



        # --------------------------------------------------------------------------------------------------------------

        InvestObjects_df["ReportInvestorID"] = (
                InvestObjects_df['NationalCode_UniversalCode'].astype(str) + '-' + InvestObjects_df['ReportID'].astype(str))

        InvestObjects_df = self.move_column_to_first(InvestObjects_df, 'NationalCode_UniversalCode')

        InvestObjects_df.drop(columns="ID", inplace=True)

        return InvestObjects_df
#
# alish = MarketMakerIssuanceCancellationPreprocessor()
# ali = alish.InvestObjects_add_required_columns()
# ali.to_excel("ali.xlsx")

class FundsInvestorsProcessor(MarketMakerIssuanceCancellationPreprocessor):
    def __init__(self):
        super().__init__()

    @DataHelper.calculate_execution_time
    def create_funds_investors_processed_helper_vfm(self):
        FundsProcessedTfm = self.build_FundsProcessedTfm()
        MarketMakerIssuanceCancellation_df = self.InvestObjects_add_required_columns()

        # ایجاد تمام ترکیبات بین دو جدول با استفاده از تابع product
        funds_investors_processed_helper_df = list(product(FundsProcessedTfm['TimeFrameReportID'], MarketMakerIssuanceCancellation_df['NationalCode_UniversalCode']))

        # ساخت DataFrame از تمام ترکیبات
        funds_investors_processed_helper_df = pd.DataFrame(funds_investors_processed_helper_df, columns=['TimeFrameReportID', 'NationalCode_UniversalCode'])

        funds_investors_processed_helper_df["ReportTimeFrameIssuanceCancellationID"] = (
                funds_investors_processed_helper_df['NationalCode_UniversalCode'].astype(str) + '-' + funds_investors_processed_helper_df['TimeFrameReportID'].astype(str))

        new_order = ['ReportTimeFrameIssuanceCancellationID', 'TimeFrameReportID', 'NationalCode_UniversalCode']
        funds_investors_processed_helper_df = funds_investors_processed_helper_df[new_order]

        # --------------------------------------------------------------------------------------------------------------
        # Add columns from FundsProcessedTfm

        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'ReportID', False)
        funds_investors_processed_helper_df["ReportInvestorID"] = (
                funds_investors_processed_helper_df['NationalCode_UniversalCode'].astype(str) + '-' + funds_investors_processed_helper_df['ReportID'].astype(str))
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'JDate', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'GDate', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'TimeFrame', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'ContractNumber', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'JYear', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'JHalfYear', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'JSeason', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'JMonthYear', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'JMonthNumber', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'JWeekNumber', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'JDayOfMonth', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'DayOfWeek', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'JalaliObject', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'ShortName', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'IranSymbol', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'HoldingName', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'TotalUnits', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'CancellationPrice', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'IssuePrice', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'PriceKey', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'AnnouncementID', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'AnnouncementType', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'ContractNumber', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'IranSymbol', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'Commitment', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'CumulativeOrderVolume', False)
        self.mapping_columns(funds_investors_processed_helper_df, FundsProcessedTfm,
                             'TimeFrameReportID', 'QuoteDomain', False)



        # --------------------------------------------------------------------------------------------------------------
        # Add columns from MarketMakerIssuanceCancellation_df

        self.mapping_columns(funds_investors_processed_helper_df, MarketMakerIssuanceCancellation_df,
                             'NationalCode_UniversalCode', 'InvestorName', False)

        # --------------------------------------------------------------------------------------------------------------
        # Add Funds columns
        self.mapping_columns(funds_investors_processed_helper_df, MarketMakerIssuanceCancellation_df,
                             'ReportInvestorID', 'FundCumSumIssuedUnitsNumber', False)

        self.mapping_columns(funds_investors_processed_helper_df, MarketMakerIssuanceCancellation_df,
                             'ReportInvestorID', 'FundCumSumIssuedAmount', False)

        """
        In this case, the "FundCumSumIssuedUnitsNumber" is not being utilized, and instead, the column "TotalUnits" from
         the table "FundsProcessedTfm" is being used. Reason being: in cases where the dates are the same, 
         calculations encounter issues.
        """

        # In this case

        self.mapping_columns(funds_investors_processed_helper_df, MarketMakerIssuanceCancellation_df,
                             'ReportInvestorID', 'FundCumSumCancellationUnitsNumber', False)

        self.mapping_columns(funds_investors_processed_helper_df, MarketMakerIssuanceCancellation_df,
                             'ReportInvestorID', 'FundCumSumCancellationAmount', False)


        # --------------------------------------------------------------------------------------------------------------
        # Add Investor Columns
        # --------------------------------------------------------------------------------------------------------------
        # Add FundsInvestor Columns

        self.mapping_columns(funds_investors_processed_helper_df, MarketMakerIssuanceCancellation_df,
                             'ReportInvestorID', 'CumSumIssuedUnitsNumber', False)

        self.mapping_columns(funds_investors_processed_helper_df, MarketMakerIssuanceCancellation_df,
                             'ReportInvestorID', 'CumSumCancellationUnitsNumber', False)

        self.mapping_columns(funds_investors_processed_helper_df, MarketMakerIssuanceCancellation_df,
                             'ReportInvestorID', 'CumSumIssuedAmount', False)

        self.mapping_columns(funds_investors_processed_helper_df, MarketMakerIssuanceCancellation_df,
                             'ReportInvestorID', 'CumSumCancellationAmount', False)



        # ==============================================================================================================
        #  Add Funds Columns
        # ==============================================================================================================
        # FundCumSumIssuedUnitsNumber
        group_cols = ["ShortName", "TimeFrame"]
        fill_cols = ["FundCumSumIssuedUnitsNumber"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)

        group_cols = ["ShortName", "TimeFrame"]
        fill_cols = ["FundCumSumIssuedAmount"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)

        # FundCumSumCancellationUnitsNumber
        group_cols = ["ShortName", "TimeFrame"]
        fill_cols = ["FundCumSumCancellationUnitsNumber"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)

        group_cols = ["ShortName", "TimeFrame"]
        fill_cols = ["FundCumSumCancellationAmount"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)
        # --------------------------------------------------------------------------------------------------------------
        # FundCumSumBuyNumber
        # group_cols = ["ShortName", "TimeFrame"]
        # fill_cols = ["FundCumSumBuyNumber"]
        # funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)
        # --------------------------------------------------------------------------------------------------------------
        # FundCumSumSellNumber
        # group_cols = ["ShortName", "TimeFrame"]
        # fill_cols = ["FundCumSumSellNumber"]
        # funds_investors_processed_helper_df = self.fillna_groupby_previous(
        #     funds_investors_processed_helper_df, group_cols, fill_cols)

        # ==============================================================================================================
        # Add Investor Columns
        # ==============================================================================================================

        # ==============================================================================================================

        # ==============================================================================================================
        # CumSumUnitsNumber
        group_cols = ["ShortName", "TimeFrame", "ReportInvestorID"]
        fill_cols = ["CumSumIssuedUnitsNumber"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)

        group_cols = ["ShortName", "TimeFrame", "NationalCode_UniversalCode"]
        fill_cols = ["CumSumIssuedUnitsNumber"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)
        # --------------------------------------------------------------------------------------------------------------
        group_cols = ["ShortName", "TimeFrame", "ReportInvestorID"]
        fill_cols = ["CumSumCancellationUnitsNumber"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)

        group_cols = ["ShortName", "TimeFrame", "NationalCode_UniversalCode"]
        fill_cols = ["CumSumCancellationUnitsNumber"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)
        # --------------------------------------------------------------------------------------------------------------
        # CumSumIssuedAmount
        group_cols = ["ShortName", "TimeFrame", "ReportInvestorID"]
        fill_cols = ["CumSumIssuedAmount"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)

        group_cols = ["ShortName", "TimeFrame", "NationalCode_UniversalCode"]
        fill_cols = ["CumSumIssuedAmount"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)

        # --------------------------------------------------------------------------------------------------------------
        # CumSumCancellationAmount
        group_cols = ["ShortName", "TimeFrame", "ReportInvestorID"]
        fill_cols = ["CumSumCancellationAmount"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)

        group_cols = ["ShortName", "TimeFrame", "NationalCode_UniversalCode"]
        fill_cols = ["CumSumCancellationAmount"]
        funds_investors_processed_helper_df = self.fillna_groupby_previous(funds_investors_processed_helper_df, group_cols, fill_cols)

        # ==============================================================================================================
        # Add Holding Columns
        # ==============================================================================================================
        # HoldingCumSumUnitsNumbers

        # ==============================================================================================================
        # Add General columns

        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        # Assuming df is the name of your DataFrame and 'ReportInvestorID' and 'UnitsNumber' are the column names you are working with
        # Finding unique UnitsNumber values for each group of records with the same ReportInvestorID
        # --------------------------------------------------------------------------------------------------------------
        # FundCumSumIssuedUnitsNumber
        unique_units_numbers = funds_investors_processed_helper_df.groupby('ReportInvestorID')['FundCumSumIssuedUnitsNumber'].transform('max')
        funds_investors_processed_helper_df['FundCumSumIssuedUnitsNumber'] = unique_units_numbers

        unique_units_numbers = funds_investors_processed_helper_df.groupby('ReportInvestorID')['FundCumSumIssuedAmount'].transform('max')
        funds_investors_processed_helper_df['FundCumSumIssuedAmount'] = unique_units_numbers

        # --------------------------------------------------------------------------------------------------------------
        # FundCumSumCancellationUnitsNumber
        unique_units_numbers = funds_investors_processed_helper_df.groupby('ReportInvestorID')['FundCumSumCancellationUnitsNumber'].transform('max')
        funds_investors_processed_helper_df['FundCumSumCancellationUnitsNumber'] = unique_units_numbers

        unique_units_numbers = funds_investors_processed_helper_df.groupby('ReportInvestorID')['FundCumSumCancellationAmount'].transform('max')
        funds_investors_processed_helper_df['FundCumSumCancellationAmount'] = unique_units_numbers

        # --------------------------------------------------------------------------------------------------------------
        # CumSumIssuedUnitsNumber
        unique_units_numbers = funds_investors_processed_helper_df.groupby('ReportInvestorID')['CumSumIssuedUnitsNumber'].transform('max')
        funds_investors_processed_helper_df['CumSumIssuedUnitsNumber'] = unique_units_numbers

        # --------------------------------------------------------------------------------------------------------------
        # CumSumCancellationUnitsNumber
        unique_units_numbers = funds_investors_processed_helper_df.groupby('ReportInvestorID')['CumSumCancellationUnitsNumber'].transform('max')
        funds_investors_processed_helper_df['CumSumCancellationUnitsNumber'] = unique_units_numbers

        # --------------------------------------------------------------------------------------------------------------
        # CumSumIssuedAmount
        unique_units_numbers = funds_investors_processed_helper_df.groupby('ReportInvestorID')['CumSumIssuedAmount'].transform('max')
        funds_investors_processed_helper_df['CumSumIssuedAmount'] = unique_units_numbers

        # --------------------------------------------------------------------------------------------------------------
        # CumSumCancellationAmount
        unique_units_numbers = funds_investors_processed_helper_df.groupby('ReportInvestorID')['CumSumCancellationAmount'].transform('max')
        funds_investors_processed_helper_df['CumSumCancellationAmount'] = unique_units_numbers

        # --------------------------------------------------------------------------------------------------------------
        # Add calculated columns

        funds_investors_processed_helper_df["FundCumSumUnitsNumber"] = funds_investors_processed_helper_df['FundCumSumIssuedUnitsNumber'] - funds_investors_processed_helper_df['FundCumSumCancellationUnitsNumber']

        funds_investors_processed_helper_df["CumSumUnitsNumber"] = funds_investors_processed_helper_df['CumSumIssuedUnitsNumber'] - funds_investors_processed_helper_df['CumSumCancellationUnitsNumber']

        funds_investors_processed_helper_df["OwnershipPercentage"] = funds_investors_processed_helper_df["CumSumUnitsNumber"] / funds_investors_processed_helper_df["TotalUnits"]

        # --------------------------------------------------------------------------------------------------------------
        # Clean Data
        funds_investors_processed_helper_df = funds_investors_processed_helper_df.drop_duplicates()
        # Drop rows where at least one of the values in specified columns is equal to zero

        # funds_investors_processed_helper_df.dropna(subset=['FundCumSumUnitsNumber'], inplace=True)
        funds_investors_processed_helper_df.dropna(subset=['CumSumIssuedUnitsNumber'], inplace=True)

        # Assuming df is the name of your DataFrame
        # Filtering records where the value in the "OwnershipPercentage" column is not equal to zero
        # funds_investors_processed_helper_df = funds_investors_processed_helper_df[funds_investors_processed_helper_df['OwnershipPercentage'] != 0]

        short_names = set(list(funds_investors_processed_helper_df["ShortName"]))

        print("after dropping")
        for short_name in short_names:
            df = self.filter_by_short_name(funds_investors_processed_helper_df, short_name)
            df = self.build_filter_by_column_value_df(df, "TimeFrame", "JDate")
            df = self.filter_by_jalali_date(df, '1402-09-27')
            percent = df["OwnershipPercentage"].sum()
            print(f'{short_name}:', percent)

        # --------------------------------------------------------------------------------------------------------------
        return funds_investors_processed_helper_df

    @DataHelper.calculate_execution_time
    def create_FundsInvestorProcessed_df(self):
        FundsInvestorsHelperTfm = self.build_FundsInvestorsProcessedHelperTfm()

        funds_investors_processed_df = FundsInvestorsHelperTfm.copy()
        FundsProcessedTfm = self.build_FundsProcessedTfm()

        # --------------------------------------------------------------------------------------------------------------
        # Add Columns from FundsInvestorsProcessedTfm

        add_columns_list = ["NumberOfStock", "NetSalesValue(FinalPrice)", "Cash_CurrentBrokerage", "Cash_BankDeposit",
                            "FundsFixedIncome", "BondsFixedIncome", "BoughtPower", "Cash", "FixedIncome", "NetCancellationAssets",
                            "FixedIncome", "AssetsValue", "NetAssetsValue", "CreditValue", "CumSumBuyNumber", "CumSumNetBuyAmount",
                            "CumSumSellNumber", "CumSumNetSellAmount", "CumSumTransactionValue", "CommitmentAmount",
                            "CumSumBuySellNumber", "CumSumNetBuySellAmount"
                            ]

        for col in add_columns_list:
            self.mapping_columns(funds_investors_processed_df, FundsProcessedTfm,
                             'TimeFrameReportID', col, False)

            # Multiplying each column by the selected multiplier column and replacing the results in the same column
            funds_investors_processed_df[col] = funds_investors_processed_df[col] * funds_investors_processed_df["OwnershipPercentage"]

        # Changing column names by adding the word "Fund"
        # ProcessedMarketMakerIssuanceCancellation_df.rename(columns={col: f"Fund{col}" for col in add_columns_list}, inplace=True)

        funds_investors_processed_df["CumSumNetInputMoney"] = funds_investors_processed_df['CumSumIssuedAmount'] - funds_investors_processed_df['CumSumCancellationAmount']

        funds_investors_processed_df["CumSumProfitLoss"] = funds_investors_processed_df["NetAssetsValue"] + funds_investors_processed_df['CumSumCancellationAmount'] - funds_investors_processed_df['CumSumIssuedAmount']
        funds_investors_processed_df["CumSumReturn"] = funds_investors_processed_df["CumSumProfitLoss"] / funds_investors_processed_df['CumSumIssuedAmount']

        funds_investors_processed_df["AverageIssuedPrice"] = funds_investors_processed_df["CumSumIssuedAmount"] / funds_investors_processed_df["CumSumIssuedUnitsNumber"]
        funds_investors_processed_df["AverageCancellationPrice"] = funds_investors_processed_df["CumSumCancellationAmount"] / funds_investors_processed_df["CumSumCancellationUnitsNumber"]

        funds_investors_processed_df["FundAverageIssuedPrice"] = funds_investors_processed_df["FundCumSumIssuedAmount"] / funds_investors_processed_df["FundCumSumIssuedUnitsNumber"]
        funds_investors_processed_df["FundAverageCancellationPrice"] = funds_investors_processed_df["FundCumSumCancellationAmount"] / funds_investors_processed_df["FundCumSumCancellationUnitsNumber"]

        return funds_investors_processed_df

class InvestorsProcessor(FundsInvestorsProcessor):
    def __init__(self):
        super().__init__()

    @DataHelper.calculate_execution_time
    def create_investors_process_vfm(self):
        FundsInvestorsProcessedTfm = self.build_FundsInvestorsProcessedTfm()
        investors_processed_vfm = FundsInvestorsProcessedTfm.groupby(["NationalCode_UniversalCode", 'InvestorName', 'JDate',
                                                                      'GDate', "TimeFrame", "JYear","JHalfYear", "JSeason",
                                                                      "JMonthYear", "JMonthNumber", "JWeekNumber", "JalaliObject",
                                                                      "JDayOfMonth", "DayOfWeek"], as_index=False).sum()

        investors_processed_vfm["CumSumNetInputMoney"] = investors_processed_vfm['CumSumIssuedAmount'] - investors_processed_vfm['CumSumCancellationAmount']

        investors_processed_vfm["InvestorTotalUnits"] = investors_processed_vfm['CumSumIssuedUnitsNumber'] - \
                                                     investors_processed_vfm['CumSumCancellationUnitsNumber']

        investors_processed_vfm["CumSumProfitLoss"] = investors_processed_vfm["NetAssetsValue"] + investors_processed_vfm['CumSumCancellationAmount'] - investors_processed_vfm['CumSumIssuedAmount']
        investors_processed_vfm["CumSumReturn"] = investors_processed_vfm["CumSumProfitLoss"] / investors_processed_vfm['CumSumIssuedAmount']

        investors_processed_vfm["AverageIssuedPrice"] = investors_processed_vfm["CumSumIssuedAmount"] / investors_processed_vfm["CumSumIssuedUnitsNumber"]
        investors_processed_vfm["AverageCancellationPrice"] = investors_processed_vfm["CumSumCancellationAmount"] / investors_processed_vfm["CumSumCancellationUnitsNumber"]

        investors_processed_vfm["FundUnitsPercentage"] = investors_processed_vfm["InvestorTotalUnits"] / investors_processed_vfm["TotalUnits"] # Todo 1402-10-18

        investors_processed_vfm["GeneralNAV"] = investors_processed_vfm["NetAssetsValue"] / investors_processed_vfm[
            "InvestorTotalUnits"]

        columns_to_drop = [
            "ReportTimeFrameIssuanceCancellationID", "TimeFrameReportID", "ReportID", "ReportInvestorID", "ShortName",
            "HoldingName", "IranSymbol", "AnnouncementType", "ContractNumber", "CancellationPrice", "IssuePrice",
            "PriceKey", "AnnouncementID", "IranSymbol", "Commitment", "CumulativeOrderVolume", "QuoteDomain"
        ]

        investors_processed_vfm.sort_values(by=['JDate'], inplace=True)

        # Get unique time frames and investor names from the DataFrame
        time_frames = list(set(investors_processed_vfm["TimeFrame"]))
        investor_names = list(set(investors_processed_vfm["InvestorName"]))
        period_nav_returns = []

        # Loop through each unique time frame
        for time_frame in time_frames:
            print(time_frame)
            try:
                # Create a DataFrame filtered by the current time frame
                period_nav_return_df = self.build_filter_by_column_value_df(investors_processed_vfm, "TimeFrame",
                                                                            time_frame)

                # Loop through each unique investor for the current time frame
                for investor in investor_names:
                    # Create a copy of the original DataFrame
                    temp_df = period_nav_return_df.copy()

                    # Filter by investor name
                    temp_df = self.build_filter_by_column_value_df(temp_df, "InvestorName", investor)

                    # Calculate PeriodNAVReturn
                    temp_df["PeriodNAVReturn"] = np.where(
                        temp_df['GeneralNAV'].shift(1).notna(),
                        ((temp_df['GeneralNAV'] - temp_df['GeneralNAV'].shift(1)) / temp_df['GeneralNAV'].shift(1)),
                        ((temp_df['GeneralNAV'] - 1000000) / 1000000)
                    )

                    # Append to the list
                    period_nav_returns.append(temp_df)

            except:
                print(f'in {time_frame} has not return')

        # Concatenate all DataFrames in the list into one DataFrame
        investors_processed_vfm = pd.concat(period_nav_returns, ignore_index=True)

        # Sort and reset the index of the final DataFrame
        investors_processed_vfm.sort_values(by=['JDate'], inplace=True)
        investors_processed_vfm.drop(columns=columns_to_drop, inplace=True)
        investors_processed_vfm.reset_index(drop=True, inplace=True)

        # investors_processed_vfm.to_excel("investors_processed_vfm.xlsx")
        return investors_processed_vfm

class HoldingsProcessor(FundsInvestorsProcessor):
    def __init__(self):
        super().__init__()

    @DataHelper.calculate_execution_time
    def create_holdings_process_vfm(self):
        FundsInvestorsProcessedTfm = self.build_FundsInvestorsProcessedTfm()
        Holdings_processed_vfm = FundsInvestorsProcessedTfm.groupby(["HoldingName", 'JDate',
                                                                      'GDate', "TimeFrame", "JYear","JHalfYear", "JSeason",
                                                                      "JMonthYear", "JMonthNumber", "JWeekNumber", "JalaliObject",
                                                                      "JDayOfMonth", "DayOfWeek"], as_index=False).sum()

        Holdings_processed_vfm["CumSumNetInputMoney"] = Holdings_processed_vfm['CumSumIssuedAmount'] - Holdings_processed_vfm['CumSumCancellationAmount']

        Holdings_processed_vfm["HoldingTotalUnits"] = Holdings_processed_vfm['CumSumIssuedUnitsNumber'] - \
                                                       Holdings_processed_vfm['CumSumCancellationUnitsNumber']

        Holdings_processed_vfm["CumSumProfitLoss"] = Holdings_processed_vfm["NetAssetsValue"] + Holdings_processed_vfm['CumSumCancellationAmount'] - Holdings_processed_vfm['CumSumIssuedAmount']
        Holdings_processed_vfm["CumSumReturn"] = Holdings_processed_vfm["CumSumProfitLoss"] / Holdings_processed_vfm['CumSumIssuedAmount']

        Holdings_processed_vfm["AverageIssuedPrice"] = Holdings_processed_vfm["CumSumIssuedAmount"] / Holdings_processed_vfm["CumSumIssuedUnitsNumber"]
        Holdings_processed_vfm["AverageCancellationPrice"] = Holdings_processed_vfm["CumSumCancellationAmount"] / Holdings_processed_vfm["CumSumCancellationUnitsNumber"]

        Holdings_processed_vfm["GeneralNAV"] = Holdings_processed_vfm["NetAssetsValue"] / Holdings_processed_vfm[
            "HoldingTotalUnits"]

        columns_to_drop = [
            "ReportTimeFrameIssuanceCancellationID", "TimeFrameReportID", "ReportID", "ReportInvestorID", "ShortName",
            "InvestorName", "IranSymbol", "AnnouncementType", "ContractNumber", "CancellationPrice", "IssuePrice",
            "PriceKey", "AnnouncementID", "IranSymbol", "Commitment", "CumulativeOrderVolume", "QuoteDomain", "FundAverageIssuedPrice",
            "FundAverageCancellationPrice", "NationalCode_UniversalCode", "FundCumSumIssuedUnitsNumber","FundCumSumIssuedAmount",
            "FundCumSumCancellationUnitsNumber", "FundCumSumCancellationAmount", "FundCumSumUnitsNumber", "TotalUnits"
        ]
        # Get unique time frames and investor names from the DataFrame
        time_frames = list(set(Holdings_processed_vfm["TimeFrame"]))
        holding_names = list(set(Holdings_processed_vfm["HoldingName"]))
        period_nav_returns = []

        # Loop through each unique time frame
        for time_frame in time_frames:
            print(time_frame)
            try:
                # Create a DataFrame filtered by the current time frame
                period_nav_return_df = self.build_filter_by_column_value_df(Holdings_processed_vfm, "TimeFrame",
                                                                            time_frame)

                # Loop through each unique investor for the current time frame
                for holding in holding_names:
                    # Create a copy of the original DataFrame
                    temp_df = period_nav_return_df.copy()

                    # Filter by investor name
                    temp_df = self.build_filter_by_column_value_df(temp_df, "HoldingName", holding)

                    # Calculate PeriodNAVReturn
                    temp_df["PeriodNAVReturn"] = np.where(
                        temp_df['GeneralNAV'].shift(1).notna(),
                        ((temp_df['GeneralNAV'] - temp_df['GeneralNAV'].shift(1)) / temp_df['GeneralNAV'].shift(1)),
                        ((temp_df['GeneralNAV'] - 1000000) / 1000000)
                    )

                    # Append to the list
                    period_nav_returns.append(temp_df)

            except:
                print(f'in {time_frame} has not return')

        # Concatenate all DataFrames in the list into one DataFrame
        Holdings_processed_vfm = pd.concat(period_nav_returns, ignore_index=True)

        # Sort and reset the index of the final DataFrame
        Holdings_processed_vfm.sort_values(by=['JDate'], inplace=True)
        Holdings_processed_vfm.drop(columns=columns_to_drop, inplace=True)
        Holdings_processed_vfm.reset_index(drop=True, inplace=True)

        return Holdings_processed_vfm

class GeneralProcessor(FundsInvestorsProcessor):
    def __init__(self):
        super().__init__()

    @DataHelper.calculate_execution_time
    def create_general_process_vfm(self):
        FundsInvestorsProcessedTfm = self.build_FundsInvestorsProcessedTfm()
        general_processed_vfm = FundsInvestorsProcessedTfm.groupby(['JDate',
                                                                      'GDate', "TimeFrame", "JYear","JHalfYear", "JSeason",
                                                                      "JMonthYear", "JMonthNumber", "JWeekNumber", "JalaliObject",
                                                                      "JDayOfMonth", "DayOfWeek"], as_index=False).sum()

        general_processed_vfm["CumSumNetInputMoney"] = general_processed_vfm['CumSumIssuedAmount'] - general_processed_vfm['CumSumCancellationAmount']

        general_processed_vfm["GeneralTotalUnits"] = general_processed_vfm['CumSumIssuedUnitsNumber'] - \
                                                     general_processed_vfm['CumSumCancellationUnitsNumber']

        general_processed_vfm["CumSumProfitLoss"] = general_processed_vfm["NetAssetsValue"] + general_processed_vfm['CumSumCancellationAmount'] - general_processed_vfm['CumSumIssuedAmount']
        general_processed_vfm["CumSumReturn"] = general_processed_vfm["CumSumProfitLoss"] / general_processed_vfm['CumSumIssuedAmount']

        general_processed_vfm["AverageIssuedPrice"] = general_processed_vfm["CumSumIssuedAmount"] / general_processed_vfm["CumSumIssuedUnitsNumber"]
        general_processed_vfm["AverageCancellationPrice"] = general_processed_vfm["CumSumCancellationAmount"] / general_processed_vfm["CumSumCancellationUnitsNumber"]

        general_processed_vfm["GeneralNAV"] = general_processed_vfm["NetAssetsValue"] / general_processed_vfm[
            "GeneralTotalUnits"]

        columns_to_drop = [
            "ReportTimeFrameIssuanceCancellationID", "TimeFrameReportID", "ReportID", "ReportInvestorID", "ShortName",
            "InvestorName", "IranSymbol", "AnnouncementType", "ContractNumber", "CancellationPrice", "IssuePrice",
            "PriceKey", "AnnouncementID", "IranSymbol", "Commitment", "CumulativeOrderVolume", "QuoteDomain", "FundAverageIssuedPrice",
            "FundAverageCancellationPrice", "NationalCode_UniversalCode", "FundCumSumIssuedUnitsNumber","FundCumSumIssuedAmount",
            "FundCumSumCancellationUnitsNumber", "FundCumSumCancellationAmount", "FundCumSumUnitsNumber", "HoldingName", "TotalUnits"
        ]

        # Get unique time frames and investor names from the DataFrame
        time_frames = list(set(general_processed_vfm["TimeFrame"]))
        period_nav_returns = []

        # Loop through each unique time frame
        for time_frame in time_frames:
            print(time_frame)
            try:
                # Create a DataFrame filtered by the current time frame
                period_nav_return_df = self.build_filter_by_column_value_df(general_processed_vfm, "TimeFrame",
                                                                            time_frame)

                # Create a copy of the original DataFrame
                temp_df = period_nav_return_df.copy()

                # Calculate PeriodNAVReturn
                temp_df["PeriodNAVReturn"] = np.where(
                    temp_df['GeneralNAV'].shift(1).notna(),
                    ((temp_df['GeneralNAV'] - temp_df['GeneralNAV'].shift(1)) / temp_df['GeneralNAV'].shift(1)),
                    ((temp_df['GeneralNAV'] - 1000000) / 1000000)
                )

                # Append to the list
                period_nav_returns.append(temp_df)

            except:
                print(f'in {time_frame} has not return')

        # Concatenate all DataFrames in the list into one DataFrame
        general_processed_vfm = pd.concat(period_nav_returns, ignore_index=True)

        # Sort and reset the index of the final DataFrame
        general_processed_vfm.sort_values(by=['JDate'], inplace=True)
        general_processed_vfm.drop(columns=columns_to_drop, inplace=True)
        general_processed_vfm.reset_index(drop=True, inplace=True)

        return general_processed_vfm

# holding = GeneralProcessor()
# holding.create_general_process_vfm()



class FundsRatio:
    pass



















import concurrent.futures
import threading

def manage_threads_start(which_op):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(which_op)


# issue = MarketMakerIssuanceCancellationPreprocessor()
# issue.create_RawMarketMakerIssuanceCancellationTbl()
# issue.create_PreprocessMarketMakerIssuanceCancellation_df()
# issue.MarketMakerIssuanceCancellation_add_required_columns()
# manage_threads_start(lambda: issue.create_PrePreprocessMarketMakerIssuanceCancellation())

# fends_investor = FundsInvestorsProcessor()
# fends_investor.create_FundInvestor_df()

# investors = InvestorsProcessor()
# investors.create_investors_process_vfm()


class AnnouncementsInformationViewFrames(FilterFramesHelper, AnnouncementsInformationPreprocessor):
    def __init__(self):
        super().__init__()

    def build_general_Announcements_vfm(self):
        AnnouncementsInformation_vfm = self.build_preprocessed_announcementsInformation_df()
        return AnnouncementsInformation_vfm

    def build_last_general_announcements_vfm(self):
        AnnouncementsInformation_vfm = self.build_general_Announcements_vfm()

        last_general_AnnouncementsInformation_vfm = self.build_last_gregorian_date_record_df(
            AnnouncementsInformation_vfm,
            'AnnouncementEffectiveGDate',
            'ShortName')
        return last_general_AnnouncementsInformation_vfm

    def build_active_general_announcements_vfm(self):
        last_general_AnnouncementsInformation_vfm = self.build_last_general_announcements_vfm()

        active_date = self.g_today
        max_date = last_general_AnnouncementsInformation_vfm['AnnouncementEffectiveGDate'].max()
        filter_conditions = [
            {'column_name': 'FinishMarketMakingGDate', 'value': active_date, 'operator': '>='},
        ]

        active_announcement_for_fund_df = self.create_filter_dataframe(last_general_AnnouncementsInformation_vfm,
                                                                       filter_conditions)
        print(active_announcement_for_fund_df.columns)
        # active_announcement_for_fund_df = active_announcement_for_fund_df.reset_index()

        return active_announcement_for_fund_df

    def fetch_events_dates(self, short_names):
        AnnouncementsInformation_vfm = self.build_general_Announcements_vfm()
        select_columns = ['AnnouncementID',
                          'ShortName',
                          'AnnouncementEffectiveJDate',
                          'AnnouncementEffectiveGDate',
                          'StartMarketMakingJDate',
                          'FinishMarketMakingJDate',
                          'FinishMarketMakingGDate']

        events_df = AnnouncementsInformation_vfm[select_columns]
        fund_events_dates_vfm = self.filter_by_short_name(events_df, short_names)

        fund_events_dates_vfm = fund_events_dates_vfm.sort_values(by='AnnouncementEffectiveJDate')

        AnnouncementEffectiveJDate_set = set(fund_events_dates_vfm['AnnouncementEffectiveJDate'])
        AnnouncementEffectiveGDate_set = set(fund_events_dates_vfm['AnnouncementEffectiveGDate'])
        FinishMarketMakingJDate_set = set(fund_events_dates_vfm['FinishMarketMakingJDate'])
        FinishMarketMakingGDate = set(fund_events_dates_vfm['FinishMarketMakingGDate'])

        return AnnouncementEffectiveJDate_set, AnnouncementEffectiveGDate_set, FinishMarketMakingJDate_set, FinishMarketMakingGDate

    def add_announcement_assets_column(self, symbols_list, j_date):
        AnnouncementsInformation_vfm = self.build_general_Announcements_vfm()
        dyr_vfm = DailyYekanReportViewFrameCreator(symbols_list, j_date)

        vfms = []
        for short_name in symbols_list:
            announcement_vfm = dyr_vfm.build_costume_vfm("AnnouncementID", 'EachSymbolsAssets', j_date=None)
            vfms.append(announcement_vfm)
