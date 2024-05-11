# developed by: Shakour Alishahi
# ======================================================================================================================
# ######################################################################################################################
# import external libraries
# ----------------------------------------------------------------------------------------------------------------------
import sqlite3
import pandas as pd
# ======================================================================================================================
# ######################################################################################################################
# import internal libraries
# ----------------------------------------------------------------------------------------------------------------------
from Materials.preprocessor_obj import DataPreprocessor
from Foundation.FilterFramesHelper import FilterFramesHelper
# ======================================================================================================================
# ######################################################################################################################
# Database call
# ----------------------------------------------------------------------------------------------------------------------


# ======================================================================================================================
# ######################################################################################################################

class BasicIranPricePreprocessor(FilterFramesHelper, DataPreprocessor):
    """
    A class for preprocessing price data from a specific database.
    Inherits from both Database and DataPreprocessor.
    """

    def __init__(self):
        """
        Initializes a new instance of the PricePreprocessor class.

        :param db_name: The name of the database to connect to.
        """
        super().__init__()
        self.db_name = f'{self.project_path}/Warehouse/IranStockDataBase.db'
        self.table_name = 'RawIranPricesTbl'
        self.column_check_duplicate = 'PriceKey'

    def build_raw_dataframe(self):
        conn = sqlite3.connect(self.db_name)
        raw_price_df = self.load_table_as_dataframe(self.table_name, conn, self.column_check_duplicate)
        conn.close()
        return raw_price_df

    def add_IranCompanyCode12_column(self):
        conn = sqlite3.connect(self.db_name)
        raw_price_df = self.build_raw_dataframe()

        basic_iran_symbol_df = self.load_table_as_dataframe('BasicIranSymbolsInformationTbl', conn, 'IranCompanyCode12')
        conn.close()

        raw_price_df = self.mapping_columns(raw_price_df,basic_iran_symbol_df,'IranSymbol','IranCompanyCode12', drop_pivot_column=False)

        # raw_price_df["IranSymbol"] = raw_price_df["PersianSymbol"]
        # raw_price_df = raw_price_df.drop(columns=["PersianSymbol"])
        #
        # raw_price_df["Date"] = raw_price_df["GDate"]
        # raw_price_df = raw_price_df.drop(columns=["Date"])

        raw_price_df = raw_price_df.rename(columns={'Date': 'GDate'})

        conn_basic_database = sqlite3.connect(f"{self.project_path}/Warehouse/BasicDataBase.db")
        df_date = self.load_table_as_dataframe("DateTbl", conn_basic_database, "GDate")
        raw_price_df = self.mapping_columns(raw_price_df, df_date, "GDate", "JDate", drop_pivot_column=False)
        conn_basic_database.close()

        raw_price_df["TransactionValue"] = ((raw_price_df["Open"]+raw_price_df["High"]+raw_price_df["Low"]+raw_price_df["Close"])/4)*raw_price_df["Volume"]
        return raw_price_df

    def calculate_adjustment_columns(self):
        df = self.add_IranCompanyCode12_column()
        df["AdjFactor"] = df["AdjClose"] / df["Close"]
        df["AdjOpen"] = df["Open"] * df["AdjFactor"]
        df["AdjHigh"] = df["High"] * df["AdjFactor"]
        df["AdjLow"] = df["Low"] * df["AdjFactor"]
        df["AdjVolume"] = df["Volume"] / df["AdjFactor"]

        df = df.rename(columns={'RawPriceKey': 'PriceKey'})
        df = df.dropna()

        new_column_order = ['PriceKey', 'GDate', 'JDate', 'TimeFrame', 'IranSymbol', 'Symbol', 'IranCompanyCode12', 'Open', 'High', 'Low',
                            'Close', 'AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose', 'Volume','AdjVolume', 'TransactionValue']

        df = df[new_column_order]

        return df

    # def filter_symbol(self, symbol):
    #     df = self.calculate_adjustment_columns()
    #     filter_conditions = [
    #         {'column_name': 'IranSymbol', 'value': f'{symbol}', 'operator': '=='},
    #     ]
    #     symbol_df = self.create_filter_dataframe(df, filter_conditions)
    #
    #     return symbol_df

    # Todo: We should also filter the date so that the speed increases and the calculations are done only on the date we want 1402/08/01

    def calculate_normalize_columns(self):
        first_df = self.calculate_adjustment_columns()
        symbol_list = set(list(first_df["IranSymbol"]))

        df = self.calculate_adjustment_columns()

        dfs_list = []
        for symbol in symbol_list:
            symbol_df = self.filter_by_iran_symbol(df, symbol)

            symbol_df = self.normalize_and_standardize_data(symbol_df, columns=['AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose', 'AdjVolume', 'TransactionValue'])
            dfs_list.append(symbol_df)
            print(symbol)

        df = pd.concat(dfs_list, axis=0, ignore_index=True)

        return df

# preprocess = BasicIranPricePreprocessor()
#
# df_list =["خودرو", "فولاد", "شتران"]
# data = preprocess.calculate_normalize_columns()
# print(data[["Symbol", "NormalizedAdjVolume"]])
#
# print(data.columns)









