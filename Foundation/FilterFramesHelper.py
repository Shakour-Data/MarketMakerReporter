import pandas as pd

from RawMaterials.data_base_obj import DataHelper
from Materials.create_df_from_tables import \
    IranStockTableFrameBuilder, BasicDataBaseTableFrame, IranMarketMakerTableFrameBuilder

class FilterFramesHelper(IranMarketMakerTableFrameBuilder, BasicDataBaseTableFrame, IranStockTableFrameBuilder):
    def __init__(self):
        super().__init__()

    def fetch_all_symbols_set(self):
        BasicIranSymbolsInformation_tfm = self.build_BasicIranSymbolsInformationTfm()
        symbol_set = set(BasicIranSymbolsInformation_tfm["Symbol"])
        short_name_set = set(BasicIranSymbolsInformation_tfm["ShortName"])
        iran_symbol_set = set(BasicIranSymbolsInformation_tfm["IranSymbol"])

        return symbol_set, short_name_set, iran_symbol_set

    def fetch_market_maker_symbols_set(self):
        MarketMakerBasicFundsInformation_tfm = self.build_MarketMakerBasicFundsInformationTfm()
        BasicIranSymbolsInformation_tfm = self.build_BasicIranSymbolsInformationTfm()

        self.mapping_columns(MarketMakerBasicFundsInformation_tfm, BasicIranSymbolsInformation_tfm, 'IranCompanyCode12',
                             'Symbol', False)

        self.mapping_columns(MarketMakerBasicFundsInformation_tfm, BasicIranSymbolsInformation_tfm, 'IranCompanyCode12',
                             'ShortName', False)

        self.mapping_columns(MarketMakerBasicFundsInformation_tfm, BasicIranSymbolsInformation_tfm, 'IranCompanyCode12',
                             'IranSymbol', False)

        market_maker_symbol_set = set(MarketMakerBasicFundsInformation_tfm["Symbol"])
        market_maker_short_name_set = set(MarketMakerBasicFundsInformation_tfm["ShortName"])
        market_maker_iran_symbol_set = set(MarketMakerBasicFundsInformation_tfm["IranSymbol"])

        return market_maker_symbol_set, market_maker_short_name_set, market_maker_iran_symbol_set

    # ##################################################################################################################

    def filter_by_short_name(self, dataframe, short_name):

        filter_frame_filtered_by_short_name = self.build_filter_by_column_value_df(dataframe, 'ShortName', short_name)

        return filter_frame_filtered_by_short_name

    @staticmethod
    def filter_by_short_names(dataframe, short_names):
        filtered_dataframe = dataframe[dataframe['ShortName'].isin(short_names)]
        return filtered_dataframe

    def filter_by_iran_symbol(self, dataframe, iran_symbol):

        filter_frame_filtered_by_iran_symbol = self.build_filter_by_column_value_df(dataframe, 'IranSymbol', iran_symbol)

        return filter_frame_filtered_by_iran_symbol

    def filter_between_two_jalali_dates(self, dataframe, start_jalali_date, end_jalali_date):
        filter_frame_filtered_between_two_jalali_dates = self.build_filter_between_two_value_df(dataframe, 'JDate',
                                                                                              start_jalali_date,
                                                                                              end_jalali_date)

        return filter_frame_filtered_between_two_jalali_dates

    def filter_by_jalali_date(self, dataframe, jalali_date):
        filter_frame_filtered_by_jalali_date = self.build_filter_by_column_value_df(dataframe, 'JDate', jalali_date)

        return filter_frame_filtered_by_jalali_date

    def filter_by_jalali_objects_date(self, dataframe, objects_date, column_value):
        # objects_date: 'JDate', 'JHalfYear', 'JSeason', 'JMonthYear' 'JMonthNumber', 'JWeekNumber',

        filter_frame_filtered_by_jalali_objects_date = self.build_filter_by_column_value_df(dataframe, objects_date, column_value)

        return filter_frame_filtered_by_jalali_objects_date

    def filter_by_n_days_ago(self, dataframe,  start_jalali_date, n):

        end_jalali_date = self.jalali_date_n_days_ago(start_jalali_date, n)

        filter_frame_filtered_by_n_days_ago = self.filter_between_two_jalali_dates(dataframe, start_jalali_date, end_jalali_date)

        return filter_frame_filtered_by_n_days_ago

    @staticmethod
    def build_last_gregorian_date_record_df(dataframe, gregorian_date_column, group_by_column):

        dataframe[f'{gregorian_date_column}_'] = pd.to_datetime(
            dataframe[gregorian_date_column])

        last_dataframe = dataframe.groupby(group_by_column).apply(
            lambda x: x.loc[x[f'{gregorian_date_column}_'].idxmax()])

        last_dataframe = last_dataframe.drop(columns=[f'{gregorian_date_column}_'])

        return last_dataframe

    def build_date_filter_for_yearly(self, dataframe, jalali_date_column, jalali_year):
        # Rename the date column with the desired name
        dataframe[jalali_date_column] = dataframe['JDate']
        Date_tfm = self.build_DateTfm()
        self.mapping_columns(dataframe, Date_tfm, 'JDate', 'JYear', False)
        self.mapping_columns(dataframe, Date_tfm, 'JDate', 'JMonthNumber', False)
        self.mapping_columns(dataframe, Date_tfm, 'JDate', 'JDayOfMonth', False)

        # Filter the dataframe by the specified jalali_year
        dataframe_filter_for_yearly = self.filter_by_jalali_objects_date(dataframe, 'JYear', jalali_year)

        # Check if the filtered dataframe is empty
        if dataframe_filter_for_yearly.empty:
            # Handle the case where no data is available for the specified year
            print(f"No data available for the year {jalali_year}.")
            # You can choose to raise an exception, return an empty dataframe, or take other actions as needed.
            return pd.DataFrame()  # Returning an empty dataframe in this example

        # Check the month column and return the maximum value
        max_month = dataframe_filter_for_yearly['JMonthNumber'].max()

        # Filter the data again by the maximum month
        dataframe_filter_for_yearly = self.filter_by_jalali_objects_date(dataframe_filter_for_yearly,
                                                                         'JMonthNumber', max_month)

        # Check the day column and return the maximum value
        max_day = dataframe_filter_for_yearly['JDayOfMonth'].max()

        # Filter the data again by the maximum day
        dataframe_filter_for_yearly = self.filter_by_jalali_objects_date(dataframe_filter_for_yearly, 'JDayOfMonth',
                                                                         max_day)

        return dataframe_filter_for_yearly

    @staticmethod
    def fetch_market_maker_symbols():
        iran_stock = IranStockTableFrameBuilder()
        symbol_tfm = iran_stock.build_BasicIranSymbolsInformationTfm()

        market_maker_ = IranMarketMakerTableFrameBuilder()
        basic_funds_information_tfm = market_maker_.build_MarketMakerBasicFundsInformationTfm()

        helper = DataHelper()
        helper.mapping_columns(basic_funds_information_tfm, symbol_tfm, 'IranCompanyCode12',
                             'ShortName', drop_pivot_column=False)

        symbols = list(basic_funds_information_tfm['ShortName'])

        return symbols

# ######################################################################################################################
# short_name = 'Pakdis Company'
# report = DailyYekanReportPreprocessor()
# rp = report.build_preprocessed_daily_report_yekan_df()
# tester = FilterFramesHelper(rp)
# # tester_df = tester.filter_by_short_name(short_name)
#
# tester_df = tester.filter_between_two_jalali_dates('1402-01-01', '1402-07-01')
#
# # tester_df = tester_df.dropna(subset=['AdjClose'], how='any')
#
# # tester_df.to_excel(f'{short_name}_daily_report.xlsx')
# tester_df.to_excel('daily_report.xlsx')