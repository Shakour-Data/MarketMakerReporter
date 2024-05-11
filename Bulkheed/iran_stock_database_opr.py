# developed by: Shakour Alishahi
# ======================================================================================================================
# ######################################################################################################################
# import external libraries
# ----------------------------------------------------------------------------------------------------------------------
import pandas as pd
import sqlite3

# ======================================================================================================================
# ######################################################################################################################
# import internal libraries
# ----------------------------------------------------------------------------------------------------------------------
from RawMaterials.data_base_obj import DataHelper
from Bulkheed.get_iran_market_data_opr import IranFinanceSource
from Foundation.price_preprocessor import BasicIranPricePreprocessor
# ======================================================================================================================
# ######################################################################################################################
# Database call
# ----------------------------------------------------------------------------------------------------------------------


# ======================================================================================================================
# ######################################################################################################################
# Create FirstIranStockSymbolListTbl table and insert data to it -> Inheritance from class DataHelper
# Database: "IranStockDataBase.db"
# ----------------------------------------------------------------------------------------------------------------------
class FirstIranStockSymbolListTblCreator(DataHelper):
    """
    Create the 'FirstIranStockSymbolListTbl' table if it doesn't exist.
    The table will have the following columns:
    - Ticker
    - Name (TEXT)
    - Market (TEXT)
    - Panel (TEXT)
    - Sector (TEXT)
    - Sub-Sector (TEXT)
    - Comment (TEXT)
    - Name(EN) (TEXT)
    - Company Code(12) (TEXT, PRIMARY KEY)
    - Ticker(4) (TEXT)
    - Ticker(5) (TEXT)
    - Ticker(12) (TEXT)
    - Sector Code (TEXT)
    - Sub-Sector Code (TEXT)
    - Panel Code (TEXT)

    Example:
    creator = FirstIranStockSymbolListTblCreator('example.db')
    creator.create_FirstIranStockSymbolListTbl()
    """
    def __init__(self, db_name, json_file):
        """
        Initialize the FirstIranStockSymbolListTblCreator.

        Args:
            db_name (str): The name of the database.
            json_file (str): The path to the JSON file.

        Returns:
            None
        """
        super().__init__()
        self.db_name = db_name
        self.json_file = json_file

    def create_FirstIranStockSymbolListTbl(self):
        """
        Create the 'FirstIranStockSymbolListTbl' table in the database.

        This function reads data from the specified JSON file, removes duplicate entries based on 'Company Code(12)',
        establishes a connection to the database, defines data types for columns, and creates the table.

        Args:
            None (Uses self.json_file and self.db_name)

        Returns:
            None
        """
        df = self.json_to_dataframe(self.json_file)
        df = df.drop_duplicates(subset=["Company Code(12)"])
        conn = sqlite3.connect(self.db_name)

        dtyp = {
            'Ticker': 'TEXT',
            'Name': 'TEXT',
            'Market': 'TEXT',
            'Panel': 'TEXT',
            'Sector': 'TEXT',
            'Sub-Sector': 'TEXT',
            'Comment': 'TEXT',
            'Name(EN)': 'TEXT',
            'Company Code(12)': 'TEXT PRIMARY KEY',
            'Ticker(4)': 'TEXT',
            'Ticker(5)': 'TEXT',
            'Ticker(12)': 'TEXT',
            'Sector Code': 'TEXT',
            'Sub-Sector Code': 'TEXT',
            'Panel Code': 'TEXT'
        }
        df.to_sql("FirstIranStockSymbolListTbl", conn, index=False, if_exists='replace', dtype=dtyp)



# ======================================================================================================================
# ######################################################################################################################
# Create RawIranSymbolsBasicInformationTbl table and insert data to it -> Inheritance from class DataHelper
# Database: "IranStockDataBase.db"
# ----------------------------------------------------------------------------------------------------------------------
class RawIranSymbolsBasicInformationTblCreator(DataHelper):
    """
    Create the 'RawIranSymbolsBasicInformationTbl' table if it doesn't exist.
    The table will have the following columns:
    - CompanyName(Persian) (TEXT)
    - Market (TEXT)
    - Panel (TEXT)
    - Sector (TEXT)
    - Sub-Sector (TEXT)
    - CompanyEnglishName (TEXT)
    - CompanyCode12 (TEXT, PRIMARY KEY)
    - Symbol (TEXT)
    - Ticker5 (TEXT)
    - Ticker12 (TEXT)
    - SectorCode (TEXT)
    - SubSectorCode Code (TEXT)
    - PanelCode Code (TEXT)

    Example:
    creator = RawIranSymbolsBasicInformationTblCreator('example.db')
    creator.create_RawIranSymbolsBasicInformationTbl()
    """
    def __init__(self, db_name):
        """
        Initialize RawIranSymbolsBasicInformationTblCreator.

        Args:
            db_name (str): The name of the database.

        Returns:
            None
        """
        self.db_name = db_name

    def create_RawIranSymbolsBasicInformationTbl(self):
        """
        Create the 'RawIranSymbolsBasicInformationTbl' table in the database.

        This function connects to the specified database, loads data from the 'FirstIranStockSymbolListTbl'
        table, performs renaming and reordering of columns, defines data types for columns, and creates the table.

        Args:
            None (Uses self.db_name)

        Returns:
            None
        """
        conn = sqlite3.connect(self.db_name)

        df = self.load_table_as_dataframe('FirstIranStockSymbolListTbl',conn, 'Company Code(12)')

        rename_dict = {
            'Name': 'CompanyPersianName',
            'Ticker': 'IranSymbol',
            'Market': 'IranMarket',
            'Panel': 'IranPanel',
            'Sector': 'IranIndustry',
            'Sub-Sector': 'IranSubIndustry',
            'Name(EN)': 'ShortName',
            'Company Code(12)': 'IranCompanyCode12',
            'Ticker(4)': 'Symbol',
            'Ticker(5)': 'Ticker5',
            'Ticker(12)': 'Ticker12',
            'Sector Code': 'IndustryIranCode',
            'Sub-Sector Code': 'SubIndustryIranCode',
            'Panel Code': 'PanelIranCode'
        }
        df.rename(columns=rename_dict, inplace=True)

        new_column_order = ['IranCompanyCode12', 'Symbol', 'ShortName','IranSymbol', 'CompanyPersianName', 'IranMarket', 'IranPanel',
                            'IranIndustry','IranSubIndustry', 'Ticker5', 'Ticker12', 'IndustryIranCode',
                            'SubIndustryIranCode','PanelIranCode']

        df = df[new_column_order]

        dtyp = {
            'IranCompanyCode12': 'TEXT PRIMARY KEY',
            'IranSymbol':'TEXT',
            'CompanyPersianName': 'TEXT',
            'IranMarket': 'TEXT',
            'IranPanel': 'TEXT',
            'IranIndustry': 'TEXT',
            'IranSubIndustry': 'TEXT',
            'ShortName': 'TEXT',
            'Symbol': 'TEXT',
            'Ticker5': 'TEXT',
            'Ticker12': 'TEXT',
            'IndustryIranCode': 'TEXT',
            'SubIndustryIranCode': 'TEXT',
            'PanelIranCode': 'TEXT'
        }
        df.to_sql("RawIranSymbolsBasicInformationTbl", conn, index=False, if_exists='replace', dtype=dtyp)

        conn.close()


# ======================================================================================================================
# ######################################################################################################################
# Create RawIranPricesTbl table and insert data to it -> Inheritance from class DataHelper
# ----------------------------------------------------------------------------------------------------------------------
class RawIranPricesTblCreator(DataHelper):
    """
    Create and populate 'RawIranPricesTbl' table.

    This class is responsible for creating a table to store raw Iran stock price data and populating it.

    The table will have the following columns:
        - RawKey (TEXT PRIMARY KEY)
        - Symbol (TEXT)
        - Date (TEXT)
        - TimeFrame (TEXT)
        - Open (REAL)
        - High (REAL)
        - Low (REAL)
        - Close (REAL)
        - AdjOpen (REAL)
        - AdjHigh (REAL)
        - AdjLow (REAL)
        - AdjClose (REAL)
        - Volume (INTEGER)

    Args:
        db_name (str): The name of the database.

    Methods:
        create_raw_iran_price_table(self): Create and populate the 'RawIranPricesTbl' table.

    Example:
        creator = RawIranPricesTblCreator('example.db')
        creator.create_raw_iran_price_table()
    """
    def __init__(self, db_name):
        """
        Initialize RawIranPricesTblCreator.

        Args:
            db_name (str): The name of the database.

        Returns:
            None
        """
        super().__init__()
        self.db_name = db_name

    def create_raw_iran_price_table(self):
        """
        Create and populate the 'RawIranPricesTbl' table.

        This method connects to the database, retrieves symbol information, fetches raw price data,
        performs necessary data transformations, defines data types for columns, and creates the table.

        Args:
            None (Uses self.db_name)

        Returns:
            None
        """
        conn = sqlite3.connect(self.db_name)
        symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn, "IranCompanyCode12")
        symbol_list = list(symbol_df["IranSymbol"])
        data_gather = IranFinanceSource(symbol_list)
        raw_iran_prices_df = data_gather.fetch_iran_stock_price_data()

        raw_iran_prices_df = self.mapping_columns(raw_iran_prices_df, symbol_df, "IranSymbol","Symbol", drop_pivot_column=False)
        raw_iran_prices_df["PriceKey"] = raw_iran_prices_df["Symbol"] + "_" + raw_iran_prices_df["Date"] + "_" + \
                                    raw_iran_prices_df["TimeFrame"]

        dtyp = {
            'PriceKey': 'TEXT  PRIMARY KEY',
            'Date': 'TEXT',
            'TimeFrame': 'TEXT',
            'Open': 'INTEGER',
            'High': 'INTEGER',
            'Low': 'INTEGER',
            'Close': 'INTEGER',
            'AdjOpen': 'INTEGER',
            'AdjHigh': 'INTEGER',
            'AdjLow': 'INTEGER',
            'AdjClose': 'INTEGER',
            'Volume': 'INTEGER',
            'IranSymbol': 'TEXT',
            'Symbol': 'TEXT'
        }

        new_column_order = ['PriceKey', 'Date', 'TimeFrame', 'IranSymbol', 'Symbol', 'Open', 'High', 'Low',
                            'Close', 'AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose',
                            'Volume']

        raw_iran_prices_df = raw_iran_prices_df[new_column_order]

        raw_iran_prices_df.to_sql("RawIranPricesTbl", conn, index=False, if_exists='replace', dtype=dtyp)

        conn.close()


# ======================================================================================================================
# ######################################################################################################################
# Create BasicIranIndustriesInformationTbl table and insert data to it -> Inheritance from class DataHelper
# ----------------------------------------------------------------------------------------------------------------------
class BasicIranIndustriesInformationTblCreator(DataHelper):
    """
    Create and populate 'BasicIranIndustriesInformationTbl' table.
    The table will have the following columns:
    - IndustryIranCode (INTEGER PRIMARY KEY AUTOINCREMENT)
    - IranIndustryName (TEXT)


    This class is responsible for creating a table to store basic information about Iran industries and populating it.

    Args:
        db_name (str): The name of the database.

    Methods:
        create_BasicIranIndustriesInformationTbl(self): Create and populate the 'BasicIranIndustriesInformationTbl' table.

    Example:
        creator = BasicIranIndustriesInformationTblCreator('example.db')
        creator.create_BasicIranIndustriesInformationTbl()
    """
    def __init__(self, db_name):
        """
        Initialize BasicIranIndustriesInformationTblCreator.

        Args:
            db_name (str): The name of the database.

        Returns:
            None
        """
        super().__init__()
        self.db_name = db_name

    def create_BasicIranIndustriesInformationTbl(self):
        """
        Create and populate the 'BasicIranIndustriesInformationTbl' table.

        This method connects to the database, retrieves basic Iran industries information,
        defines data types for columns, and creates the table.

        Args:
            None (Uses self.db_name)

        Returns:
            None
        """
        conn = sqlite3.connect(self.db_name)

        df = self.load_table_as_dataframe('RawIranSymbolsBasicInformationTbl', conn, "IranCompanyCode12")
        df = df.dropna()

        # Create a DataFrame with unique sector names
        iran_industries_df = pd.DataFrame()
        iran_industries_df["IndustryIranCode"] = df["IndustryIranCode"]
        iran_industries_df["IndustryIranName"] = df["IranIndustry"]
        iran_industries_df = iran_industries_df.drop_duplicates(subset=['IndustryIranCode'])

        dtyp = {
            "IndustryIranCode": "INTEGER PRIMARY KEY",
            "IndustryIranName": "TEXT"
        }

        iran_industries_df.to_sql("BasicIranIndustriesInformationTbl", conn, index=False, if_exists='replace', dtype=dtyp)

        conn.close()


        # Todo Mapping with yahoo_finance standard Todo delete 1402/07/04 ->1402/09/30

# ======================================================================================================================
# ######################################################################################################################
# Create BasicIranSubIndustryInformationTbl table and insert data to it -> Inheritance from class DataHelper
# ----------------------------------------------------------------------------------------------------------------------
class BasicIranSubIndustriesInformationTblCreator(DataHelper):
    """
    Create and populate 'BasicIranSubIndustriesInformationTbl' table.
    The table will have the following columns:
    "SubIndustryIranCode": "INTEGER PRIMARY KEY",
    "SubIndustryName": "TEXT",
    "IndustryIranCode": (INTEGER, Foreign Key to BasicIndustriesInformationTbl),

    This class is responsible for creating a table to store basic information about Iran sub-industries and populating it.

    Args:
        db_name (str): The name of the database.

    Methods:
        create_BasicIndustriesInformationTbl(self): Create and populate the 'BasicIranSubIndustriesInformationTbl' table.

    Example:
        creator = BasicIranSubIndustriesInformationTblCreator('example.db')
        creator.create_BasicIndustriesInformationTbl()
    """
    def __init__(self, db_name):
        """
        Initialize BasicIranSubIndustriesInformationTblCreator.

        Args:
            db_name (str): The name of the database.

        Returns:
            None
        """
        super().__init__()
        self.db_name = db_name

    def create_BasicIndustriesInformationTbl(self):
        """
        Create and populate the 'BasicIranSubIndustriesInformationTbl' table.

        This method connects to the database, retrieves basic Iran sub-industries information,
        defines data types for columns, and creates the table.

        Args:
            None (Uses self.db_name)

        Returns:
            None
        """
        conn = sqlite3.connect(self.db_name)

        # Read data from RawSymbolsBasicInformationTbl table
        df = self.load_table_as_dataframe("RawIranSymbolsBasicInformationTbl", conn, "IranCompanyCode12")
        df = df.dropna()

        iran_sub_industries_df = pd.DataFrame()
        iran_sub_industries_df["SubIndustryIranCode"] = df["SubIndustryIranCode"]
        iran_sub_industries_df["SubIndustryName"] = df["IranSubIndustry"]
        iran_sub_industries_df["IndustryIranCode"] = df["IndustryIranCode"]
        iran_sub_industries_df = iran_sub_industries_df.drop_duplicates(subset=['SubIndustryIranCode'])

        # Define the data types for the new table
        dtyp = {
            "SubIndustryIranCode": "INTEGER PRIMARY KEY",
            "SubIndustryName": "TEXT",
            "IndustryIranCode": "INTEGER",
        }

        iran_sub_industries_df = iran_sub_industries_df.dropna()
        # Insert data into BasicIndustriesInformationTbl
        iran_sub_industries_df.to_sql("BasicIranSubIndustriesInformationTbl", conn, index=False, if_exists='replace',
                             dtype=dtyp)

        # Close connections
        conn.close()


# ======================================================================================================================
# ######################################################################################################################
# Create BasicIranMarketsInformationTbl table and insert data to it -> Inheritance from class DataHelper
# ----------------------------------------------------------------------------------------------------------------------
class BasicIranMarketsInformationTblCreator(DataHelper):
    """
    Create and populate 'BasicIranMarketsInformationTbl' table.
    The table will have the following columns:


    This class is responsible for creating a table to store basic information about Iran markets and populating it.

    Args:
        db_name (str): The name of the database.

    Methods:
        create_BasicIranMarketsInformationTblCreator(self): Create and populate the 'BasicIranMarketsInformationTbl' table.

    Example:
        creator = BasicIranMarketsInformationTblCreator('example.db')
        creator.create_BasicIranMarketsInformationTblCreator()
    """
    def __init__(self, db_name):
        """
        Initialize BasicIranMarketsInformationTblCreator.

        Args:
            db_name (str): The name of the database.

        Returns:
            None
        """
        super().__init__()
        self.db_name = db_name

    def create_BasicIranMarketsInformationTblCreator(self):
        """
        Create and populate the 'BasicIranMarketsInformationTbl' table.

        This method connects to the database, retrieves basic Iran markets information,
        defines data types for columns, and creates the table.

        Args:
            None (Uses self.db_name)

        Returns:
            None
        """
        conn = sqlite3.connect(self.db_name)

        # Read data from RawSymbolsBasicInformationTbl table
        df = self.load_table_as_dataframe("RawIranSymbolsBasicInformationTbl", conn, "IranCompanyCode12")
        df = df.dropna()

        iran_market_df = pd.DataFrame()

        iran_market_df["IranMarketName"] = df["IranMarket"]

        iran_market_df = iran_market_df.drop_duplicates(subset=['IranMarketName'])
        iran_market_df["IranMarketID"] = range(1, len(iran_market_df) + 1)

        # Define the data types for the new table
        dtyp = {
            "IranMarketID": "INTEGER PRIMARY KEY",
            "IranMarketName": "TEXT",
        }

        new_column_order = ["IranMarketID", "IranMarketName"]
        iran_market_df = iran_market_df[new_column_order]

        iran_market_df = iran_market_df.dropna()
        # Insert data into BasicIndustriesInformationTbl
        iran_market_df.to_sql("BasicIranMarketsInformationTbl", conn, index=False, if_exists='replace',
                                      dtype=dtyp)

        # Close connections
        conn.close()


# ======================================================================================================================
# ######################################################################################################################
# Create BasicIranSymbolsInformationTbl table and insert data to it -> Inheritance from class DataHelper
# Database: "IranStockDataBase.db"
# ----------------------------------------------------------------------------------------------------------------------
class BasicIranSymbolsInformationTbl(DataHelper):
    """
    Create and populate 'BasicIranSymbolsInformationTbl' table.
    The table will have the following columns:


    This class is responsible for creating a table to store basic information about Iran symbols and populating it.

    Args:
        db_name (str): The name of the database.

    Methods:
        create_BasicIranSymbolsInformationTbl(self): Create and populate the 'BasicIranSymbolsInformationTbl' table.

    Example:
        creator = BasicIranSymbolsInformationTbl('example.db')
        creator.create_BasicIranSymbolsInformationTbl()
    """
    def __init__(self, db_name):
        """
        Initialize BasicIranSymbolsInformationTbl.

        Args:
            db_name (str): The name of the database.

        Returns:
            None
        """
        super().__init__()
        self.db_name = db_name

    def create_BasicIranSymbolsInformationTbl(self):
        """
        Create and populate the 'BasicIranSymbolsInformationTbl' table.


        This method connects to the database, retrieves basic Iran symbols information,
        performs necessary data transformations, defines data types for columns, and creates the table.

        Args:
            None (Uses self.db_name)

        Returns:
            None
        """
        conn = sqlite3.connect(self.db_name)

        df = self.load_table_as_dataframe('RawIranSymbolsBasicInformationTbl', conn, "IranCompanyCode12")
        df = df.dropna()

        df_market = self.load_table_as_dataframe('BasicIranMarketsInformationTbl', conn, "IranMarketID")

        # Create a DataFrame with unique sector names
        df_symbol = pd.DataFrame()
        df_symbol["IranCompanyCode12"] = df["IranCompanyCode12"]
        df_symbol["Symbol"] = df["Symbol"]
        df_symbol["ShortName"] = df["ShortName"]
        df_symbol["IranSymbol"] = df["IranSymbol"]
        df_symbol["CompanyPersianName"] = df["CompanyPersianName"]
        df_symbol["IranMarketName"] = df["IranMarket"]
        df_symbol["IndustryIranCode"] = df["IndustryIranCode"]
        df_symbol["SubIndustryIranCode"] = df["SubIndustryIranCode"]

        df_symbol = self.mapping_columns(df_symbol, df_market, "IranMarketName", "IranMarketID", drop_pivot_column=True)

        df_symbol = df_symbol.drop_duplicates(subset=["IranCompanyCode12"], keep="first")


        dtyp = {
            "IranCompanyCode12": "TEXT PRIMARY KEY",
            "Symbol": "TEXT",
            "ShortName": "TEXT",
            "IranSymbol": "TEXT",
            "CompanyPersianName": "TEXT",
            "IranMarketID": "INTEGER",
            "IndustryIranCode": "INTEGER",
            "SubIndustryIranCode": "INTEGER"
        }

        df_symbol.to_sql("BasicIranSymbolsInformationTbl", conn, index=False, if_exists='replace', dtype=dtyp)

        conn.close()


# ======================================================================================================================
# ######################################################################################################################
# Create PreprocessedIranMarketPricesTbl table and insert data to it -> Inheritance from class DataHelper
# Database: "IranStockDataBase.db"
# ----------------------------------------------------------------------------------------------------------------------

class PreprocessedIranMarketPricesTblCreator(DataHelper):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_PreprocessedIranMarketPricesTbl(self):
        conn = sqlite3.connect(self.db_name)

        preprocess = BasicIranPricePreprocessor()
        df = preprocess.calculate_normalize_columns()


        dtyp = {
            'PriceKey': 'TEXT PRIMARY KEY',
            'GDate': 'TEXT',
            'JDate': 'TEXT',
            'TimeFrame': 'TEXT',
            'IranSymbol': 'TEXT',
            'Symbol': 'TEXT',
            'IranCompanyCode12': 'TEXT',
            'Open': 'INTEGER',
            'High': 'INTEGER',
            'Low': 'INTEGER',
            'Close': 'INTEGER',
            'AdjOpen': 'INTEGER',
            'AdjHigh': 'INTEGER',
            'AdjLow': 'INTEGER',
            'AdjClose': 'INTEGER',
            'Volume': 'INTEGER',
            'AdjVolume': 'INTEGER',
            'TransactionValue': 'INTEGER',
            'NormalizedAdjOpen': 'INTEGER',
            'NormalizedAdjHigh': 'INTEGER',
            'NormalizedAdjLow': 'INTEGER',
            'NormalizedAdjClose': 'INTEGER',
            'NormalizedAdjVolume': 'INTEGER',
            'NormalizedTransactionValue': 'INTEGER'
        }

        df.to_sql("PreprocessedIranMarketPricesTbl", conn, index=False, if_exists='replace', dtype=dtyp)
        conn.close()
# ======================================================================================================================
# ######################################################################################################################
# Create RawIranIndividualCorporateTransactionsTbl table and insert data to it -> Inheritance from class DataHelper
# Database: "IranStockDataBase.db"
# ----------------------------------------------------------------------------------------------------------------------

class RawIranIndividualCorporateTransactionsTblCreator(DataHelper):

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_RawIranIndividualCorporateTransactionsTbl(self):

        conn = sqlite3.connect(self.db_name)
        conn_market_maker = sqlite3.connect(f"{self.project_path}/main_create_database/IranMarketMaker.db")
        symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn, "IranCompanyCode12")

        symbol_list = list(symbol_df["IranSymbol"])

        data_gather = IranFinanceSource(symbol_list)
        raw_iran_individual_corporate_df = data_gather.fetch_iran_stock_individual_corporate_transactions_data()

        raw_iran_individual_corporate_df = self.mapping_columns(raw_iran_individual_corporate_df, symbol_df, "IranSymbol", "Symbol", drop_pivot_column=False)
        raw_iran_individual_corporate_df["PriceKey"] = raw_iran_individual_corporate_df["Symbol"] + "_" + raw_iran_individual_corporate_df["Date"] + "_" + \
                                                          raw_iran_individual_corporate_df["TimeFrame"]

        dtyp = {
            'PriceKey': 'TEXT  PRIMARY KEY',
            'Date': 'TEXT',
            'TimeFrame': 'TEXT',
            'IranSymbol': 'TEXT',
            'individual_buy_count': 'INTEGER',
            'individual_sell_count': 'INTEGER',
            'corporate_buy_count': 'INTEGER',
            'corporate_sell_count': 'INTEGER',
            'individual_buy_vol': 'INTEGER',
            'individual_sell_vol': 'INTEGER',
            'corporate_buy_vol': 'INTEGER',
            'corporate_sell_vol': 'INTEGER',
            'individual_buy_value': 'INTEGER',
            'individual_sell_value': 'INTEGER',
            'corporate_buy_value': 'INTEGER',
            'corporate_sell_value': 'INTEGER',
            'Symbol': 'TEXT',
        }

        column_order = ["PriceKey", "Date", "TimeFrame", "IranSymbol", "Symbol","individual_buy_count",
                        "individual_sell_count", "corporate_buy_count", "corporate_sell_count", "individual_buy_vol",
                        "individual_sell_vol", "corporate_buy_vol", "corporate_sell_vol", "individual_buy_value",
                        "individual_sell_value", "corporate_buy_value", "corporate_sell_value",
                        ]

        raw_iran_individual_corporate_df = raw_iran_individual_corporate_df[column_order]

        raw_iran_individual_corporate_df = raw_iran_individual_corporate_df.drop_duplicates(
            subset=["PriceKey"])

        raw_iran_individual_corporate_df.to_sql("RawIranIndividualCorporateTransactionsTbl", conn, index=False, if_exists='replace', dtype=dtyp)

        conn_market_maker.close()
        conn.close()

class RawIranStockShareHoldersTblCreator(DataHelper):
    # Todo: This class works correctly, but it is very time-consuming and cannot be used
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_RawIranStockShareHoldersTbl(self):

        conn = sqlite3.connect(self.db_name)
        conn_market_maker = sqlite3.connect("../main_create_database/IranMarketMaker.db")
        symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn, "IranCompanyCode12")
        symbol_list_df = self.load_table_as_dataframe("MarketMakerBasicFundsInformationTbl", conn_market_maker, "IranCompanyCode12")
        symbol_list = list(symbol_list_df["SymbolFundYekan"])
        # symbol_list = list(symbol_df["IranSymbol"])

        data_gather = IranFinanceSource(symbol_list,"1d","2023-09-25","2023-09-29")
        raw_iran_stock_share_holders_df = data_gather.fetch_iran_stock_share_holders_data()

        raw_iran_stock_share_holders_df = self.mapping_columns(raw_iran_stock_share_holders_df, symbol_df, "IranSymbol", "Symbol", drop_pivot_column=False)
        raw_iran_stock_share_holders_df["PriceKey"] = raw_iran_stock_share_holders_df["Symbol"] + "_" + raw_iran_stock_share_holders_df["Date"] + "_" + \
                                                                       raw_iran_stock_share_holders_df["TimeFrame"]

        dtyp = {
            'PriceKey': 'TEXT  PRIMARY KEY',
            'Date': 'TEXT',
            'TimeFrame': 'TEXT',
            'IranSymbol': 'TEXT',
            'shareholder_id': 'TEXT',
            'shareholder_shares': 'INTEGER',
            'shareholder_percentage': 'INTEGER',
            'IranCompanyCode12': 'TEXT',
            'shareholder_name': 'INTEGER',
            "change": 'INTEGER',

        }

        column_order = ["PriceKey", "Date", "TimeFrame", "IranSymbol", "Symbol", "shareholder_id",
                        "shareholder_shares",
                        "shareholder_percentage", "IranCompanyCode12", "shareholder_name", "change"
                        ]

        raw_iran_stock_share_holders_df = raw_iran_stock_share_holders_df[column_order]

        raw_iran_stock_share_holders_df = raw_iran_stock_share_holders_df.drop_duplicates(
            subset=["RawStockShareHoldersKey"])

        raw_iran_stock_share_holders_df.to_sql("RawIranStockShareHoldersTbl", conn, index=False, if_exists='replace', dtype=dtyp)

        conn_market_maker.close()
        conn.close()


class IranStockFloatingSharesTblCreator(DataHelper):
    # Todo: This class is not fully written. complete it 1402/08/12
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_IranStockFloatingSharesTbl(self):
        conn = sqlite3.connect(self.db_name)
        conn_market_maker = sqlite3.connect("../main_create_database/IranMarketMaker.db")
        symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn, "IranCompanyCode12")
        # symbol_df = self.load_table_as_dataframe("MarketMakerBasicFundsInformationTbl", conn_market_maker, "IranCompanyCode12")
        symbol_list = list(symbol_df["IranSymbol"])
        data_gather = IranFinanceSource(symbol_list,"1d","2000-01-01", "2023-09-29")
        iran_stock_floating_shares_df = data_gather.fetch_iran_stock_floating_share_data()

        iran_stock_floating_shares_df = self.mapping_columns(iran_stock_floating_shares_df, symbol_df, "IranSymbol", "Symbol", drop_pivot_column=False)
        iran_stock_floating_shares_df["PriceKey"] = iran_stock_floating_shares_df["Symbol"] + "_" + iran_stock_floating_shares_df["Date"] + "_" + \
                                                             iran_stock_floating_shares_df["TimeFrame"]

        dtyp = {
            'PriceKey': 'TEXT  PRIMARY KEY',
            'Date': 'TEXT',
            'TimeFrame': 'TEXT',
            'IranSymbol': 'TEXT',
            "Symbol":"TEXT",
            "FloatingShares": 'INTEGER',
        }

        column_order = ["PriceKey", "Date", "TimeFrame", "IranSymbol", "Symbol", "FloatingShares",
                        ]

        iran_stock_floating_shares_df = iran_stock_floating_shares_df[column_order]

        iran_stock_floating_shares_df = iran_stock_floating_shares_df.drop_duplicates(
            subset=["PriceKey"])
        iran_stock_floating_shares_df = iran_stock_floating_shares_df.dropna(
            subset=["PriceKey"])

        iran_stock_floating_shares_df.to_sql("IranStockFloatingSharesTbl", conn, index=False, if_exists='append', dtype=dtyp)

        conn_market_maker.close()
        conn.close()


class IranStockKeyStatesTblCreator(DataHelper):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_IranStockKeyStatesTbl(self):
        conn = sqlite3.connect(self.db_name)
        conn_market_maker = sqlite3.connect("../main_create_database/IranMarketMaker.db")
        data_gather = IranFinanceSource([])
        key_states_df = data_gather.fetch_iran_stock_key_stats()
        symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn, "IranCompanyCode12")
        key_states_df = self.mapping_columns(key_states_df, symbol_df, "IranSymbol",
                                                               "Symbol", drop_pivot_column=False)

        key_states_df["PriceKey"] = key_states_df["Symbol"] + "_" + key_states_df["Date"] + "_" + \
                                                                       key_states_df["TimeFrame"]

        key_states_df = key_states_df.dropna(subset=['KeyStatesID'])

        key_states_df.to_sql("IranStockKeyStatesTbl", conn, index=False, if_exists='replace')

        conn_market_maker.close()
        conn.close()


class RawIranStockIntraMarketWatchTblCreator(DataHelper):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_RawIntraMarketWatchTbl(self):
        conn = sqlite3.connect(self.db_name)
        conn_market_maker = sqlite3.connect("../main_create_database/IranMarketMaker.db")
        symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn, "IranCompanyCode12")
        # symbol_df = self.load_table_as_dataframe("MarketMakerBasicFundsInformationTbl", conn_market_maker, "IranCompanyCode12")
        symbol_list = list(symbol_df["IranSymbol"])

        data_gather = IranFinanceSource(symbol_list, "1d", "2000-01-01", self.today_date_as_string()[0])
        raw_intra_market_watch_df, raw_intra_order_book_df = data_gather.fetch_intra_tse_market_watch()

        raw_intra_market_watch_df = raw_intra_market_watch_df.rename(columns={'Ticker': 'IranSymbol', 'Download': 'JDownloadDateTime','Time':'TseUpdateTime'})
        raw_intra_order_book_df = raw_intra_order_book_df.rename(columns={'Ticker': 'IranSymbol', 'Download': 'JDownloadDateTime'})

        self.add_converted_datetime(raw_intra_market_watch_df, "jalali_to_gregorian", 'JDownloadDateTime', 'GDownloadDateTime')
        self.add_converted_datetime(raw_intra_order_book_df, "jalali_to_gregorian", 'JDownloadDateTime', 'GDownloadDateTime')

        self.split_datetime(raw_intra_market_watch_df, "GDownloadDateTime", '%Y-%m-%d', '%H:%M:%S', 'GDate', 'Time', False)
        self.split_datetime(raw_intra_order_book_df, "GDownloadDateTime", '%Y-%m-%d', '%H:%M:%S', 'GDate', 'Time', False)

        raw_intra_market_watch_df = self.mapping_columns(raw_intra_market_watch_df, symbol_df, "IranSymbol", "Symbol", drop_pivot_column=False)
        raw_intra_market_watch_df["IntraMarketWatchKey"] = raw_intra_market_watch_df["Symbol"] + "_" + raw_intra_market_watch_df["GDate"] + "_" + raw_intra_market_watch_df["Time"]

        raw_intra_order_book_df = self.mapping_columns(raw_intra_order_book_df, symbol_df, "IranSymbol", "Symbol", drop_pivot_column=False)
        raw_intra_order_book_df["IntraMarketWatchKey"] = raw_intra_order_book_df["Symbol"] + "_" + raw_intra_order_book_df["GDate"] + "_" + raw_intra_order_book_df["Time"]

        raw_intra_order_book_df['OB-Depth'] = raw_intra_order_book_df['OB-Depth'].astype(str)
        raw_intra_order_book_df["IntraBookOrderKey"] = raw_intra_order_book_df["Symbol"] + "_" + raw_intra_order_book_df["GDate"] + "_" + \
                                                       raw_intra_order_book_df["Time"] + "_" + raw_intra_order_book_df["OB-Depth"]

        raw_intra_market_watch_df = raw_intra_market_watch_df.drop_duplicates(
            subset=["IntraMarketWatchKey"])
        raw_intra_market_watch_df = raw_intra_market_watch_df.dropna(
            subset=["IntraMarketWatchKey"])

        raw_intra_order_book_df = raw_intra_order_book_df.drop_duplicates(
            subset=["IntraBookOrderKey"])
        raw_intra_order_book_df = raw_intra_order_book_df.dropna(
            subset=["IntraBookOrderKey"])

        raw_intra_market_watch_df.to_sql("RawIranStockIntraMarketWatchTbl", conn, index=False, if_exists='append')
        raw_intra_order_book_df.to_sql("RawIranStockIntraOrderBookTbl", conn, index=False, if_exists='append')

        conn_market_maker.close()
        conn.close()

class IranStockIntraMarketWatchTblCreator(DataHelper):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_IntraMarketWatchTbl(self):
        conn = sqlite3.connect(self.db_name)
        intra_market_watch_df = self.load_table_as_dataframe("RawIranStockIntraMarketWatchTbl", conn, "IntraMarketWatchKey")
        symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn, "IranCompanyCode12")

        intra_market_watch_df.rename(columns={'Time': 'IntraTseUpdateTime'})

        self.mapping_columns(intra_market_watch_df, symbol_df, "IranSymbol", "IranCompanyCode12", False )

        intra_market_watch_df = intra_market_watch_df.drop(columns=['BQPC', 'SQPC', 'Market', 'Sector'])
        self.add_converted_date(intra_market_watch_df, "gregorian_to_jalali", 'GDate', 'JDate')

        rename_market_watch_dict = {
            'IranSymbol': 'IranSymbol',
            'Trade Type': 'TradeType',
            'IntraTseUpdateTime': 'IntraTseUpdateTime',
            'Open': 'IntraOpen',
            'High': 'IntraHigh',
            'Low': 'IntraLow',
            'Close': 'IntraClose',
            'Final': 'IntraFinal',
            'Close(%)': 'IntraClose(%)',
            'Final(%)': 'IntraFinal(%)',
            'Day_UL': 'DailyMaxAllowPrice',
            'Day_LL': 'DailyMinAllowPrice',
            'BQ-Value': 'BQ-Value',
            'SQ-Value': 'SQ-Value',
            'Volume': 'IntraVolume',
            'Vol_Buy_R': 'IntraIndividualBuyVolume',
            'Vol_Buy_I': 'IntraCorporateBuyVolume',
            'Vol_Sell_R': 'IntraIndividualSellVolume',
            'Vol_Sell_I': 'IntraCorporateSellVolume',
            'No': 'IntraTradeCount',
            'No_Buy_R': 'IntraIndividualBuyCount',
            'No_Buy_I': 'IntraCorporateBuyCount',
            'No_Sell_R': 'IntraIndividualSellCount',
            'No_Sell_I': 'IntraCorporateSellCount',
            'Name': 'CompanyPersianName',
            'Share-No': 'ShareNumber',
            'Base-Vol': 'BaseVolume',
            'Market Cap': 'MarketCap',
            'EPS': 'EPS',
            'JDownloadDateTime': 'JDownloadDateTime',
            'GDate': 'GDate',
            'Time': 'Time',
            'Symbol': 'Symbol',
            'IntraMarketWatchKey': 'IntraMarketWatchKey',
            'JDate': 'JDate',
            'IranCompanyCode12': 'IranCompanyCode12'
        }

        intra_market_watch_df.rename(columns=rename_market_watch_dict, inplace=True)

        dtyp = {
            'IntraMarketWatchKey': 'TEXT  PRIMARY KEY',
            'JDownloadDateTime':'TEXT',
            'GDate': 'TEXT',
            'JDate': 'TEXT',
            'Time': 'TEXT',
            'TseUpdateTime': 'TEXT',
            'IranCompanyCode12': 'TEXT',
            'Symbol': 'TEXT',
            'IranSymbol': 'TEXT',
            'CompanyPersianName': 'TEXT',
            'TradeType': 'TEXT',
            'IntraOpen': 'REAL' ,
            'IntraHigh': 'REAL' ,
            'IntraLow': 'REAL',
            'IntraClose': 'REAL',
            'IntraFinal': 'REAL',
            'IntraClose(%)': 'REAL',
            'IntraFinal(%)': 'REAL',
            'IntraVolume': 'INTEGER',
            'IntraTradeCount': 'INTEGER',
            'DailyMaxAllowPrice': 'REAL',
            'DailyMinAllowPrice': 'REAL',
            'IntraIndividualBuyVolume': 'INTEGER',
            'IntraCorporateBuyVolume': 'INTEGER',
            'IntraIndividualSellVolume': 'INTEGER',
            'IntraCorporateSellVolume': 'INTEGER',
            'IntraIndividualBuyCount': 'INTEGER',
            'IntraCorporateBuyCount': 'INTEGER',
            'IntraIndividualSellCount': 'INTEGER',
            'IntraCorporateSellCount': 'INTEGER',
            'ShareNumber': 'INTEGER',
            'BaseVolume': 'INTEGER',
            'MarketCap': 'REAL',
            'EPS': 'REAL',
            'BQ-Value': 'REAL',
            'SQ-Value': 'REAL',
        }

        column_order = ['IntraMarketWatchKey', 'JDownloadDateTime', 'GDate', 'JDate', 'Time', 'TseUpdateTime', 'IranCompanyCode12', 'Symbol',
                        'IranSymbol','CompanyPersianName', 'TradeType', 'IntraOpen', 'IntraHigh', 'IntraLow', 'IntraClose', 'IntraFinal',
                        'IntraClose(%)', 'IntraFinal(%)', 'IntraVolume', 'IntraTradeCount', 'DailyMaxAllowPrice', 'DailyMinAllowPrice',
                        'IntraIndividualBuyVolume', 'IntraCorporateBuyVolume', 'IntraIndividualSellVolume', 'IntraCorporateSellVolume',
                        'IntraIndividualBuyCount', 'IntraCorporateBuyCount', 'IntraIndividualSellCount', 'IntraCorporateSellCount',
                        'ShareNumber', 'BaseVolume', 'MarketCap', 'EPS', 'BQ-Value', 'SQ-Value'
                        ]

        intra_market_watch_df = intra_market_watch_df[column_order]

        self.round_time_column_to_strings(intra_market_watch_df, 'Time',5)

        intra_market_watch_df.to_sql("IranStockIntraMarketWatchTbl", conn, index=False, if_exists='replace',
                                             dtype=dtyp)
        conn.close()


class IranStockIntraOrderBookTblCreator(DataHelper):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_IntraOrderBookTbl(self):
        conn = sqlite3.connect(self.db_name)
        intra_order_book_df = self.load_table_as_dataframe("RawIranStockIntraOrderBookTbl", conn, "IntraBookOrderKey")
        symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn, "IranCompanyCode12")

        self.mapping_columns(intra_order_book_df, symbol_df, "IranSymbol", "IranCompanyCode12", False)

        self.add_converted_date(intra_order_book_df, "gregorian_to_jalali", 'GDate', 'JDate')

        rename_market_watch_dict = {
            'IranSymbol': 'IranSymbol',
            'OB-Depth': 'OrderBookDepth',
            'Sell-No': 'SellCount',
            'Sell-Vol': 'SellVolume',
            'Sell-Price': 'SellPrice',
            'Buy-Price': 'BuyPrice',
            'Buy-Vol': 'BuyVolume',
            'Buy-No': 'BuyCount',
            'JDownloadDateTime': 'JDownloadDateTime',
            'GDate': 'GDate',
            'Time': 'Time',
            'Symbol': 'Symbol',
            'IntraMarketWatchKey': 'IntraMarketWatchKey',
            'IntraBookOrderKey': 'IntraBookOrderKey',
            'JDate': 'JDate'
        }

        intra_order_book_df.rename(columns=rename_market_watch_dict, inplace=True)
        dtyp = {
            'IntraBookOrderKey': 'TEXT  PRIMARY KEY',
            'JDownloadDateTime':'TEXT',
            'GDate': 'TEXT',
            'JDate': 'TEXT',
            'Time': 'TEXT',
            'IntraMarketWatchKey': 'TEXT',
            'Symbol': 'TEXT',
            'IranSymbol': 'TEXT',
            'CompanyPersianName': 'TEXT',
            'OrderBookDepth': 'INTEGER',
            'SellCount': 'INTEGER',
            'SellVolume': 'INTEGER',
            'SellPrice': 'REAL',
            'BuyCount': 'INTEGER',
            'BuyVolume': 'INTEGER',
            'BuyPrice': 'REAL',
        }

        column_order = [
                        'IntraBookOrderKey', 'JDownloadDateTime', 'GDate', 'JDate', 'Time', 'IntraMarketWatchKey', 'Symbol',
                        'IranSymbol', 'OrderBookDepth', 'SellCount', 'SellVolume', 'SellPrice',
                        'BuyCount', 'BuyVolume', 'BuyPrice'
                        ]

        intra_order_book_df = intra_order_book_df[column_order]
        self.round_time_column_to_strings(intra_order_book_df, 'Time', 5)

        intra_order_book_df.to_sql("IranStockIntraOrderBookTblCreator", conn, index=False, if_exists='replace',
                                             dtype=dtyp)

        conn.close()


class IranStockIntraHistoricalOrderBookTblCreator(DataHelper):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name

    def create_IntraHistoricalOrderBookTbl(self):
        # Todo
        conn = sqlite3.connect(self.db_name)
        conn_market_maker = sqlite3.connect("../main_create_database/IranMarketMaker.db")
        # symbol_df = self.load_table_as_dataframe("BasicIranSymbolsInformationTbl", conn, "IranCompanyCode12")

        symbol_df = self.load_table_as_dataframe("MarketMakerBasicFundsInformationTbl", conn_market_maker, "IranCompanyCode12")
        symbol_list = list(symbol_df["IranSymbol"])

        data_gather = IranFinanceSource(symbol_list,"1d","2023-09-01","2023-09-29")
        intra_historical_order_book_df = data_gather.fetch_historical_order_book()

        self.mapping_columns(intra_historical_order_book_df, symbol_df, "IranSymbol", "IranCompanyCode12", False)
        self.add_converted_date(intra_historical_order_book_df, "gregorian_to_jalali", 'GDate', 'JDate')

        intra_historical_order_book_df = self.mapping_columns(intra_historical_order_book_df, symbol_df, "IranSymbol", "Symbol",
                                                         drop_pivot_column=False)

        intra_historical_order_book_df["JDownloadDateTime"] = intra_historical_order_book_df["JDate"] + ' ' + \
                                                              intra_historical_order_book_df["Time"]

        intra_historical_order_book_df["IntraMarketWatchKey"] = intra_historical_order_book_df["Symbol"] + "_" + \
                                                         intra_historical_order_book_df["GDate"] + "_" + \
                                                         intra_historical_order_book_df["Time"]

        intra_historical_order_book_df["IntraBookOrderKey"] = intra_historical_order_book_df["Symbol"] + "_" + intra_historical_order_book_df["GDate"] + "_" + \
                                                       intra_historical_order_book_df["Time"] + "_" + intra_historical_order_book_df["Depth"].astype(str)

        """
        Todo This function was not completed because it should be rand for every 5 seconds and, for example,
        the average seconds including those 5 seconds should be obtained -> groupBy and filter
        """
        rename_historical_depth_dict = {
            'IranSymbol': 'IranSymbol',
            'Depth': 'OrderBookDepth',
            'Sell_No': 'SellCount',
            'Sell_Vol': 'SellVolume',
            'Sell_Price': 'SellPrice',
            'Buy_Price': 'BuyPrice',
            'Buy_Vol': 'BuyVolume',
            'Buy_No': 'BuyCount',
            'JDownloadDateTime': 'JDownloadDateTime',
            'GDate': 'GDate',
            'Time': 'Time',
            'Symbol': 'Symbol',
            'IntraMarketWatchKey': 'IntraMarketWatchKey',
            'IntraBookOrderKey': 'IntraBookOrderKey',
            'JDate': 'JDate'
        }

        intra_historical_order_book_df.rename(columns=rename_historical_depth_dict, inplace=True)
        dtyp = {
            'IntraBookOrderKey': 'TEXT  PRIMARY KEY',
            'JDownloadDateTime':'TEXT',
            'GDate': 'TEXT',
            'JDate': 'TEXT',
            'Time': 'TEXT',
            'IntraMarketWatchKey': 'TEXT',
            'Symbol': 'TEXT',
            'IranSymbol': 'TEXT',
            'CompanyPersianName': 'TEXT',
            'OrderBookDepth': 'INTEGER',
            'SellCount': 'INTEGER',
            'SellVolume': 'INTEGER',
            'SellPrice': 'REAL',
            'BuyCount': 'INTEGER',
            'BuyVolume': 'INTEGER',
            'BuyPrice': 'REAL',
        }

        column_order = [
                        'IntraBookOrderKey', 'JDownloadDateTime', 'GDate', 'JDate', 'Time', 'IntraMarketWatchKey', 'Symbol',
                        'IranSymbol', 'OrderBookDepth', 'SellCount', 'SellVolume', 'SellPrice',
                        'BuyCount', 'BuyVolume', 'BuyPrice'
                        ]

        intra_historical_order_book_df = intra_historical_order_book_df[column_order]

        intra_historical_order_book_df = intra_historical_order_book_df.drop_duplicates(subset=["IntraBookOrderKey"],
                                                                                        keep='last')

        intra_historical_order_book_df.to_sql("IranStockIntraOrderBookTblCreator", conn, index=False, if_exists='replace',
                                              dtype=dtyp)

        conn.close()
        conn_market_maker.close()



# ######################################################################################################################
# Create BasicIranStandardSymbolsInformationTbl table and insert data to it -> Inheritance from class DataHelper
# Database: "IranStockDataBase.db"
# ----------------------------------------------------------------------------------------------------------------------
# Todo 1402/07/03 -> 1402/09/30
# ======================================================================================================================
# ######################################################################################################################
# Create BasicDynamicIranStandardSymbolsInformationTbl table and insert data to it -> Inheritance from class DataHelper
# Database: "IranStockDataBase.db"
# ----------------------------------------------------------------------------------------------------------------------
# شناوری - ارزش بازار - تعداد سهام -


# ======================================================================================================================
# ######################################################################################################################
# Create HistoricalShareHolderTbl table and insert data to it -> Inheritance from class DataHelper
# Database: "IranStockDataBase.db"
# ----------------------------------------------------------------------------------------------------------------------


