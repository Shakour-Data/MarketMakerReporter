# -> developed by: Shakour Alishahi
# ======================================================================================================================
# ######################################################################################################################
# -> import external libraries
# ----------------------------------------------------------------------------------------------------------------------
import pandas as pd
import sqlite3
# ======================================================================================================================
# ######################################################################################################################
# -> import internal libraries
# ----------------------------------------------------------------------------------------------------------------------
# --- import get data from data source libraries
# ----------------------------------------------------------------------------------------------------------------------


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ---  import create and insert data to database libraries
# ----------------------------------------------------------------------------------------------------------------------
#  for Iran Market
from Bulkheed.iran_stock_database_opr import (
    FirstIranStockSymbolListTblCreator,
    RawIranSymbolsBasicInformationTblCreator,
    BasicIranIndustriesInformationTblCreator,
    BasicIranSubIndustriesInformationTblCreator,
    BasicIranMarketsInformationTblCreator,
    BasicIranSymbolsInformationTbl,
    RawIranPricesTblCreator, PreprocessedIranMarketPricesTblCreator, RawIranIndividualCorporateTransactionsTblCreator,
    RawIranStockShareHoldersTblCreator, IranStockKeyStatesTblCreator, IranStockFloatingSharesTblCreator,
    RawIranStockIntraMarketWatchTblCreator, IranStockIntraMarketWatchTblCreator, IranStockIntraOrderBookTblCreator,
    IranStockIntraHistoricalOrderBookTblCreator
)

# ======================================================================================================================
# ######################################################################################################################
# -> Database call
# ----------------------------------------------------------------------------------------------------------------------
# raw_db = "RawDataBase.db"
# basic_db = "BasicDataBase.db"
# iran_db = "IranStockDataBase.db"
# market_maker_db = "IranMarketMaker.db"
# ======================================================================================================================
class IranMarketDatabaseManager:
    def __init__(self, db_name):
        """
        Initialize IranDatabaseManager.

        Args:
            db_name (str): The name of the database.

        Returns:
            None
        """
        self.db_name = db_name
        self.project_path = "/home/shakour/shakour/Programming/Codes/GitStudy/MarketMakerReporter"
        self.db_path = "Warehouse"
        self.db_name =  self.project_path + "/" + self.db_path + "/" + self.db_name


    def create_first_iran_stock_symbol_list_table(self):
        """
        Create the 'FirstIranStockSymbolListTbl' table and insert data into it.

        Args:
            None

        Returns:
            None
        """
        json_file_path = f"{self.project_path}/Mines/FirstIranSymbolList_Stock.json"
        creator = FirstIranStockSymbolListTblCreator(self.db_name, json_file_path)
        creator.create_FirstIranStockSymbolListTbl()
        print(f"FirstIranStockSymbolListTbl created and data inserted successfully in {self.db_name}.")

    def create_raw_iran_symbol_basic_information_table(self):
        """
        Create the 'RawIranSymbolsBasicInformationTbl' table and insert data into it.

        Args:
            None

        Returns:
            None
        """
        creator = RawIranSymbolsBasicInformationTblCreator(self.db_name)
        creator.create_RawIranSymbolsBasicInformationTbl()

    def create_basic_iran_industries_information_table(self):
        """
        Create the 'BasicIranIndustriesInformationTbl' table and insert data into it.

        Args:
            None

        Returns:
            None
        """
        creator = BasicIranIndustriesInformationTblCreator(self.db_name)
        creator.create_BasicIranIndustriesInformationTbl()
        print(f"BasicIranIndustriesInformationTbl created and data inserted successfully in {self.db_name}.")

    def create_basic_iran_sub_industries_information_table(self):
        """
        Create the 'BasicIranSubIndustriesInformationTbl' table and insert data into it.

        Args:
            None

        Returns:
            None
        """
        creator = BasicIranSubIndustriesInformationTblCreator(self.db_name)
        creator.create_BasicIndustriesInformationTbl()
        print(f"BasicIranSubIndustriesInformationTbl created and data inserted successfully in in {self.db_name}.")

    def create_basic_iran_markets_information_table(self):
        """
        Create the 'BasicIranMarketsInformationTbl' table and insert data into it.

        Args:
            None

        Returns:
            None
        """
        creator = BasicIranMarketsInformationTblCreator(self.db_name)
        creator.create_BasicIranMarketsInformationTblCreator()
        print(f"BasicIranMarketsInformationTbl created and data inserted successfully in {self.db_name}.")

    def create_basic_iran_symbols_information_table(self):
        """
        Create the 'BasicIranSymbolsInformationTbl' table and insert data into it.

        Args:
            None

        Returns:
            None
        """
        creator = BasicIranSymbolsInformationTbl(self.db_name)
        creator.create_BasicIranSymbolsInformationTbl()
        print(f"BasicIranSymbolsInformationTbl created and data inserted successfully in {self.db_name}.")

    def create_basic_iran_standard_symbols_information_table(self):
        """
        Create the 'BasicIranStandardSymbolsInformationTbl' table and insert data into it.

        Args:
            None

        Returns:
            None
        """
        pass  # This section is for future development and needs implementation.
    def create_raw_iran_prices_table(self):
        creator = RawIranPricesTblCreator(self.db_name)
        creator.create_raw_iran_price_table()
        print(f"RawIranPricesTbl created and data inserted successfully in {self.db_name}.")

    def create_preprocessed_iran_prices(self):
        creator = PreprocessedIranMarketPricesTblCreator(self.db_name)
        creator.create_PreprocessedIranMarketPricesTbl()
        print(f"PreprocessedIranMarketPricesTbl created and data inserted successfully in {self.db_name}.")

    def create_raw_iran_individual_corporate_transactions_table(self):
        creator = RawIranIndividualCorporateTransactionsTblCreator(self.db_name)
        creator.create_RawIranIndividualCorporateTransactionsTbl()
        print(f"RawIranIndividualCorporateTransactionsTbl created and data inserted successfully in {self.db_name}.")

    def create_raw_iran_stock_share_holders_table(self):
        # Todo: This class works correctly, but it is very time-consuming and cannot be used
        creator = RawIranStockShareHoldersTblCreator(self.db_name)
        creator.create_RawIranStockShareHoldersTbl()
        print(f"RawIranStockShareHoldersTbl created and data inserted successfully in {self.db_name}.")

    def create_iran_stock_key_states_Creator(self):
        creator = IranStockKeyStatesTblCreator(self.db_name)
        creator.create_IranStockKeyStatesTbl()
        print(f"IranStockKeyStatesTbl created and data inserted successfully in {self.db_name}.")

    def create_iran_floating_shares_table(self):
        creator = IranStockFloatingSharesTblCreator(self.db_name)
        creator.create_IranStockFloatingSharesTbl()

    def create_raw_iran_market_watch_table(self):
        creator = RawIranStockIntraMarketWatchTblCreator(self.db_name)
        creator.create_RawIntraMarketWatchTbl()

    def create_iran_intra_market_watch_table(self):
        creator = IranStockIntraMarketWatchTblCreator(self.db_name)
        creator.create_IntraMarketWatchTbl()

    def create_iran_market_intra_order_book_table(self):
        creator = IranStockIntraOrderBookTblCreator(self.db_name)
        creator.create_IntraOrderBookTbl()

    def create_iran_stock_intra_historical_order_book_table(self):
        creator = IranStockIntraHistoricalOrderBookTblCreator(self.db_name)
        creator.create_IntraHistoricalOrderBookTbl()





iran_db = IranMarketDatabaseManager("IranStockDataBase.db")
iran_db.create_first_iran_stock_symbol_list_table()
iran_db.create_raw_iran_symbol_basic_information_table()
iran_db.create_basic_iran_industries_information_table()
iran_db.create_basic_iran_sub_industries_information_table()
iran_db.create_basic_iran_markets_information_table()
iran_db.create_basic_iran_symbols_information_table()
iran_db.create_raw_iran_prices_table() #
iran_db.create_preprocessed_iran_prices() #

# iran_db.create_basic_iran_standard_symbols_information_table()
# The following line is not implemented yet.
# iran_db.create_raw_iran_individual_corporate_transactions_table()
# iran_db.create_raw_iran_stock_share_holders_table() # *Todo: This class works correctly, but it is very time-consuming and cannot be used
# iran_db.create_iran_stock_key_states_Creator()
# iran_db.create_iran_floating_shares_table() #*
# iran_db.create_raw_iran_market_watch_table()
# iran_db.create_iran_intra_market_watch_table()
# iran_db.create_iran_market_intra_order_book_table()
# iran_db.create_iran_stock_intra_historical_order_book_table() # Todo does not complete
# iran_db.create_iran_stock_financial_physics_table()


# iran_db.create_raw_iran_prices_table() #
# iran_db.create_preprocessed_iran_prices() #
# iran_db.create_raw_iran_individual_corporate_transactions_table()
# iran_db.create_iran_stock_key_states_Creator()
# iran_db.create_iran_floating_shares_table() #
# iran_db.create_iran_market_intra_order_book_table()
# iran_db.create_iran_stock_intra_historical_order_book_table() # Todo does not complete 1402/08/11 -> 1402/08/12
# iran_db.create_iran_stock_financial_physics_table()