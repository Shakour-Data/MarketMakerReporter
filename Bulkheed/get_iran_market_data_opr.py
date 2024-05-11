# developed by: Shakour Alishahi
# ======================================================================================================================
########################################################################################################################
# import external libraries
# ----------------------------------------------------------------------------------------------------------------------
import pandas as pd
import finpy_tse as fpy
import datetime
from datetime import timedelta
import pytse_client as tse
import asyncio
from pytse_client import download_client_types_records
import logging
from pytse_client import get_stats
# ======================================================================================================================
########################################################################################################################
# import internal libraries
# ----------------------------------------------------------------------------------------------------------------------

from RawMaterials.data_base_obj import DataHelper
# ======================================================================================================================
# ######################################################################################################################
# A class for fetching and processing financial data related to Iran's stock market and indices.
# ----------------------------------------------------------------------------------------------------------------------
class IranFinanceSource(DataHelper):
    """
    A class for fetching and processing financial data related to Iran's stock market and indices.

    Args:
        symbols (list): List of stock symbols to fetch data for.
        price_interval (str, optional): The time interval for price data (e.g., '1d' for daily). Default is '1d'.
        start_date (str, optional): The start date for fetching data. Default is "2000-01-01".
        end_date (str, optional): The end date for fetching data. Default is today's date.

    Attributes:
        symbols_list (list): List of unique stock symbols.
        timeframe (str): The selected time interval for price data.
        start_date (str): The selected start date for data fetching.
        end_date (str): The selected end date for data fetching.
        jalali_start_date (str): The selected start date in Jalali (Persian) calendar.
        jalali_end_date (str): The selected end date in Jalali (Persian) calendar.

    Methods:
        fetch_iran_stock_price_data(json_filename): Fetches Iran stock price data and saves it in a JSON file.
        fetch_iran_stock_indices_data(json_filename): Fetches Iran stock indices data and saves it in a JSON file.
        fetch_iran_stock_industrial_indices_data(json_filename): Fetches Iran industrial indices data and saves it in a JSON file.
        gregorian_to_jalali(gregorian_date): Converts Gregorian date to Jalali date.
    """
    def __init__(self, symbols, price_interval=None, start_date=None, end_date=None):
        """
        Initializes the IranFinanceSource class with the given parameters.
        Args:
            symbols (list): List of financial symbols or stock symbols.
            price_interval (str, optional): The interval for prices (default is '1d' for one day).
            start_date (str, optional): The start date for data (default is "2000-01-01").
            end_date (str, optional): The end date for data (default is today's date).
        """
        super().__init__()
        self.symbols_list = list(set(symbols))
        self.timeframe = price_interval if price_interval else '1d'

        self.start_date = start_date if start_date else "2000-01-01"
        self.end_date = end_date if end_date else datetime.date.today().strftime("%Y-%m-%d")

        print(f"self.start_date: {self.start_date}")
        print(f"self.end_date: {self.end_date}")

        self.days_difference = (
                    datetime.datetime.strptime(self.end_date, "%Y-%m-%d") - datetime.datetime.strptime(self.start_date,
                                                                                                       "%Y-%m-%d")).days

        # Convert Gregorian dates to Jalali dates manually
        self.jalali_start_date = self.gregorian_to_jalali(self.start_date)
        self.jalali_end_date = self.gregorian_to_jalali(self.end_date)
    # ------------------------------------------------------------------------------------------------------------------
    def fetch_iran_stock_price_data(self):
        """
        Fetches Iran stock price data, processes it, and saves it in a JSON file.
        Returns:
            pd.DataFrame: The combined and processed dataframe containing stock price data.

        """
        dataframes = []
        for symbol in self.symbols_list:
            try:
                stock_data = fpy.get_price_history(stock=symbol, start_date=self.jalali_start_date,
                                                   end_date=self.jalali_end_date, ignore_date=False,
                                                   adjust_price=True, show_weekday=True, double_date=True)
                stock_data = stock_data.reset_index()
                stock_data["Date"] = stock_data["Date"].dt.strftime("%Y-%m-%d")
                stock_data["IranSymbol"] = symbol
                stock_data["TimeFrame"] = self.timeframe
                stock_data["AdjOpen"] = stock_data["Adj Open"]
                stock_data["AdjHigh"] = stock_data["Adj High"]
                stock_data["AdjLow"] = stock_data["Adj Low"]
                stock_data["AdjClose"] = stock_data["Adj Close"]

                dataframes.append(stock_data)
                print(symbol)

            except Exception as e:
                print(f' download {symbol} error: {e}')

        combined_dataframe = pd.concat(dataframes, ignore_index=True)
        column_order = ["RawPriceKey", "IranSymbol", "Date", "TimeFrame", "Open", "High", "Low", "Close", "AdjOpen",
                        "AdjHigh", "AdjLow", "AdjClose", "Volume"]
        prices_dataframe = combined_dataframe.reindex(columns=column_order)

        return prices_dataframe

    # ------------------------------------------------------------------------------------------------------------------

    def fetch_iran_stock_indices_data(self):
        """
        Fetches Iran industrial indices data, processes it, and saves it in a JSON file.

        Returns:
            pd.DataFrame: The combined and processed dataframe containing industrial indices data.
        """
        def create_indices_data_frame_helper(data_frame, index_name):
            data_frame = data_frame.reset_index()
            data_frame["Date"] = data_frame["Date"].dt.strftime("%Y-%m-%d")
            data_frame["Symbol"] = index_name
            data_frame["TimeFrame"] = self.timeframe
            data_frame["AdjClose"] = data_frame["Adj Close"]
            data_frame["RawIndexKey"] = data_frame["Symbol"] + "_" + data_frame["Date"] + "_" + \
                                      data_frame["TimeFrame"]

            return data_frame

        dataframes = []
        # --------------------
        # Add TEPIX Index
        tepix_df = fpy.Get_CWI_History(
            start_date=self.jalali_start_date,
            end_date=self.jalali_end_date,
            ignore_date=False,
            just_adj_close=False,
            show_weekday=False,
            double_date=True)

        tepix_df = create_indices_data_frame_helper(tepix_df, "CWI" )

        dataframes.append(tepix_df)
        # --------------------
        # Add KolHamvazn Index
        kol_hamvazn_df = fpy.Get_EWI_History(
            start_date=self.jalali_start_date,
            end_date=self.jalali_end_date,
            ignore_date=False,
            just_adj_close=False,
            show_weekday=False,
            double_date=True)

        kol_hamvazn_df = create_indices_data_frame_helper(kol_hamvazn_df,"EWI")
        dataframes.append(kol_hamvazn_df)
        # --------------------
        # Add vazni_arzeshi_df Index
        vazni_arzeshi_df = fpy.Get_CWPI_History(
            start_date=self.jalali_start_date,
            end_date=self.jalali_end_date,
            ignore_date=False,
            just_adj_close=False,
            show_weekday=False,
            double_date=True)

        vazni_arzeshi_df = create_indices_data_frame_helper(vazni_arzeshi_df, "CWPI")
        dataframes.append(vazni_arzeshi_df)
        # --------------------
        # Add AzadShenavar Index
        AzadShenavar_df = fpy.Get_FFI_History(
            start_date=self.jalali_start_date,
            end_date=self.jalali_end_date,
            ignore_date=False,
            just_adj_close=False,
            show_weekday=False,
            double_date=True)

        AzadShenavar_df = create_indices_data_frame_helper(AzadShenavar_df, "FFI")
        dataframes.append(AzadShenavar_df)
        # --------------------
        # Add BazarAval Index
        BazarAval_df = fpy.Get_MKT1I_History(
            start_date=self.jalali_start_date,
            end_date=self.jalali_end_date,
            ignore_date=False,
            just_adj_close=False,
            show_weekday=False,
            double_date=True)

        BazarAval_df = create_indices_data_frame_helper(BazarAval_df , "MKT1I")
        dataframes.append(BazarAval_df )
        # --------------------
        # Add BazarDovom Index
        BazarDovom_df = fpy.Get_MKT2I_History(
            start_date=self.jalali_start_date,
            end_date=self.jalali_end_date,
            ignore_date=False,
            just_adj_close=False,
            show_weekday=False,
            double_date=True)

        BazarDovom_df = create_indices_data_frame_helper(BazarDovom_df , "MKT2I")
        dataframes.append(BazarDovom_df)
        # --------------------
        # Add Sanat Index
        Sanat_df = fpy.Get_INDI_History(
            start_date=self.jalali_start_date,
            end_date=self.jalali_end_date,
            ignore_date=False,
            just_adj_close=False,
            show_weekday=False,
            double_date=True)

        Sanat_df = create_indices_data_frame_helper(Sanat_df , "INDI")
        dataframes.append(Sanat_df)
        # --------------------
        # Add Sherkat50 Index
        Sherkat50_df = fpy.Get_ACT50_History(
            start_date=self.jalali_start_date,
            end_date=self.jalali_end_date,
            ignore_date=False,
            just_adj_close=False,
            show_weekday=False,
            double_date=True)

        Sherkat50_df = create_indices_data_frame_helper(Sherkat50_df , "ACT50")
        dataframes.append(Sherkat50_df)
        # --------------------
        # Add Sherkat30 Index
        Sherkat30_df = fpy.Get_LCI30_History(
            start_date=self.jalali_start_date,
            end_date=self.jalali_end_date,
            ignore_date=False,
            just_adj_close=False,
            show_weekday=False,
            double_date=True)

        Sherkat30_df = create_indices_data_frame_helper(Sherkat30_df , "LCI30")
        dataframes.append(Sherkat30_df)

        combined_dataframe = pd.concat(dataframes, ignore_index=True)
        column_order = ["RawIndexKey", "Symbol", "Date", "TimeFrame", "Open", "High", "Low", "Close", "AdjClose", "Volume"]
        combined_dataframe = combined_dataframe.reindex(columns=column_order)

        # self.dataframe_to_json(combined_dataframe, json_filename)
        return combined_dataframe
    # ------------------------------------------------------------------------------------------------------------------

    def fetch_iran_stock_industrial_indices_data(self):
        """
        Fetches Iran industrial indices data, processes it, and saves it in a JSON file.
        Returns:
            pd.DataFrame: The combined and processed dataframe containing industrial indices data.
        """
        sectors_list = ['زراعت', 'ذغال سنگ', 'کانی فلزی', 'سایر معادن', 'منسوجات', 'محصولات چرمی', 'محصولات چوبی', 'محصولات کاغذی',
         'انتشار و چاپ', 'فرآورده های نفتی', 'لاستیک',
         'فلزات اساسی', 'محصولات فلزی', 'ماشین آلات', 'دستگاه های برقی', 'وسایل ارتباطی', 'خودرو', 'قند و شکر',
         'چند رشته ای', 'تامین آب، برق و گاز', 'غذایی',
         'دارویی', 'شیمیایی', 'خرده فروشی', 'کاشی و سرامیک', 'سیمان', 'کانی غیر فلزی', 'سرمایه گذاری', 'بانک',
         'سایر مالی', 'حمل و نقل',
         'رادیویی', 'مالی', 'اداره بازارهای مالی', 'انبوه سازی', 'رایانه', 'اطلاعات و ارتباطات', 'فنی مهندسی',
         'استخراج نفت', 'بیمه و بازنشستگی']

        dataframes = []
        for indx in sectors_list:
            sector_index_data = fpy.Get_SectorIndex_History(sector=indx, start_date=self.jalali_start_date,
                                               end_date=self.jalali_end_date, ignore_date=False,
                                               just_adj_close=False, show_weekday=True, double_date=True)
            sector_index_data = sector_index_data.reset_index()
            sector_index_data["Date"] = sector_index_data["Date"].dt.strftime("%Y-%m-%d")
            sector_index_data["Symbol"] = indx
            sector_index_data["TimeFrame"] = self.timeframe
            sector_index_data["AdjClose"] = sector_index_data["Adj Close"]
            sector_index_data["RawPriceKey"] = sector_index_data["Symbol"] + "_" + sector_index_data["Date"] + "_" + \
                                        sector_index_data["TimeFrame"]
            dataframes.append(sector_index_data)

        combined_dataframe = pd.concat(dataframes, ignore_index=True)
        column_order = ["RawPriceKey", "Symbol", "Date", "TimeFrame", "Open", "High", "Low", "Close","AdjClose","Volume"]
        combined_dataframe = combined_dataframe.reindex(columns=column_order)

        return combined_dataframe

    @staticmethod
    def non_information_symbols():
        non_dict = {
                    'غدیس': '3492952121304423',
                    'گکوثر': '66599109405217136',
                    'زفجر': '36844527173896115',
                    'غمایه': '59461185672081215',
                    'وشمال': '9761381741308262',
                    'کتوسعه': '23374429962331387',
                    'تکیمیا': '66643284949247248',
                    'بمولد': '48261930411425125',
                    'نیان': '20652241232631918',
                    'ثنور': '63315013743060811',
                    'نشار': '49129081625829210',
                    'ثقزوی': '12965822877128721',
                    'فرود': '51017863148152520',
                    'درازی': '16567465928886309',
                    'شهر': '9098178887955847',
                    'سلار': '61664227282090067',
                    'فن افزار': '69171897374421261',
                    'کایزد': '58810336532668771',
                    'والماس': '36282416082320053',
                    'پرسپولیس': '55289848471625247',
                    'وسالت': '23175320865252772',
                    'استقلال': '22259718159702272',
                    'ورازی': '60079434631497942',
                    'تفارس': '22276798221643766',
                    'شستان': '3173544097113770',
                    'حاریا': '56798822689379375',
                    'وآیند': '20626178773287666',
                    'ناما': '10411249540376641',
                    'وثخوز': '40043919653526083',
                    'واحیا': '17284166795866794',
                    'فصبا': '23557166059925779',
                    'عالیس': '34213522001938649',
                    'فزرین': '43716452378323683',
                    'گشان': '70391097626818082'
                    }
        return non_dict

    def select_tickers(self, symbol):
        non_dict = self.non_information_symbols()
        try:
            ticker = tse.Ticker(symbol)
            return ticker
        except:
            if symbol in non_dict.keys():
                ticker = tse.Ticker("", index=non_dict[symbol])
                return ticker
            else:
                floating_share = 'NA'  # یا هر مقدار دلخواه دیگر
                print(f"{symbol} is not supported")

    def fetch_csv_individual_corporate_files(self):
        max_runs = 1000
        current_run = 0
        downloaded_symbols = set()

        total_symbols = len(self.symbols_list)

        while current_run < max_runs and len(downloaded_symbols) < total_symbols:
            try:
                for symbol in self.symbols_list:
                    if symbol not in downloaded_symbols:
                        download_client_types_records(symbol, write_to_csv=True)
                        downloaded_symbols.add(symbol)

                current_run += 1

                print(f'Progress: {len(downloaded_symbols)} out of {total_symbols} symbols downloaded.')

            except Exception as e:
                print(f'An error occurred - {symbol}: {e}')

    def fetch_iran_stock_individual_corporate_transactions_data(self):

        self.fetch_csv_individual_corporate_files()

        dataframes = []
        for symbol in self.symbols_list:
            try:
                df = self.read_csv_to_dataframe(f"{self.project_path}/main_create_database/client_types_data",symbol)  # استفاده از get برای بررسی وجود کلید
                df = df.drop(columns=['Unnamed: 0'])
                if df is not None:
                    df["IranSymbol"] = symbol
                    df["TimeFrame"] = self.timeframe
                    df = df.rename(columns={"date": "Date"})
                    df["RawIndividualCorporateKey"] = df["IranSymbol"] + "_" + df["Date"] + "_" + df["TimeFrame"]
                    dataframes.append(df)
                    print(symbol)

                else:
                    print(f"No data available for symbol {symbol}")
            except KeyError as e:
                logging.error(f"KeyError occurred for symbol {symbol}: {e}")
                print(f"KeyError occurred for symbol {symbol}: {e}")
            except Exception as e:
                logging.error(f"An error occurred for symbol {symbol}: {e}")
                print(f"An error occurred for symbol {symbol}: {e}")

        if dataframes:  # چک کردن اینکه آیا لیست دیتافریم‌ها خالی نیست
            combined_dataframe = pd.concat(dataframes, ignore_index=True)
            column_order = ["RawIndividualCorporateKey", "Date", "TimeFrame", "IranSymbol",
                            "individual_buy_count",
                            "individual_sell_count", "corporate_buy_count", "corporate_sell_count",
                            "individual_buy_vol",
                            "individual_sell_vol", "corporate_buy_vol", "corporate_sell_vol", "individual_buy_value",
                            "individual_sell_value", "corporate_buy_value", "corporate_sell_value",
                            ]
            combined_dataframe = combined_dataframe.reindex(columns=column_order)

            return combined_dataframe
        else:
            return None  # در صورتی که هیچ دیتافریمی ساخته نشده باشد، None برگردانید

    def fetch_iran_stock_share_holders_data(self):
        # Todo: This function works correctly, but it is very time-consuming and cannot be used
        dataframes = []
        for symbol in self.symbols_list:
            try:
                ticker = self.select_tickers(symbol)
                df = ticker.get_shareholders_history(
                    from_when=datetime.timedelta(days=self.days_difference),
                    to_when=datetime.datetime.now(),
                    only_trade_days=True,
                )

                df["IranSymbol"] = symbol
                df["TimeFrame"] = self.timeframe
                df = df.rename(columns={"date": "Date", "shareholder_instrument_id": "IranCompanyCode12"})
                df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
                df["RawStockShareHoldersKey"] = df["IranSymbol"] + "_" + df["Date"] + "_" + df["TimeFrame"]
                dataframes.append(df)
                print(symbol)

            except Exception as e:
                logging.error(f"An error occurred for symbol {symbol}: {e}")
                print(f"An error occurred for symbol {symbol}: {e}")
                print(f"Skipping symbol {symbol}")

        if dataframes:  # اگر لیست دیتافریم‌ها خالی نباشد
            combined_dataframe = pd.concat(dataframes, ignore_index=True)
            column_order = ["RawStockShareHoldersKey", "Date", "TimeFrame", "IranSymbol", "shareholder_id",
                            "shareholder_shares",
                            "shareholder_percentage", "IranCompanyCode12", "shareholder_name", "change"
                            ]
            combined_dataframe = combined_dataframe.reindex(columns=column_order)
            return combined_dataframe
        else:
            return None  # اگر لیست خالی بود، می‌توانید اینجا یک مقدار دیگر برگردانید یا None

    def fetch_iran_stock_floating_share_data(self):
        repeat = 50  # تعداد تکرار پیش‌فرض
        downloaded_data = False
        for _ in range(repeat):
            if not downloaded_data:
                floating_share_df = pd.DataFrame()
                floating_share_df["IranSymbol"] = self.symbols_list

                floating_share_df = pd.concat([floating_share_df, pd.DataFrame({
                    "Date": [datetime.datetime.today().date().strftime('%Y-%m-%d')] * len(floating_share_df),
                    "TimeFrame": [self.timeframe] * len(floating_share_df)
                })], axis=1)

                non_dict = self.non_information_symbols()

                floating_share_list = []
                non_list = []
                index_list = []  # اضافه کردن لیستی برای اطمینان از همخوانی انداکس‌ها
                for symbol in self.symbols_list:
                    if symbol not in non_dict:
                        try:
                            ticker = self.select_tickers(symbol)
                            holders = ticker.shareholders.percentage.sum()
                            floating_share = 100 - holders
                            print(symbol)
                            floating_share_list.append(floating_share)
                            index_list.append(symbol)  # افزودن ایندکس به لیست
                        except:
                            print(f'{symbol} data is not available')
                            floating_share_list.append(None)
                            index_list.append(symbol)  # افزودن ایندکس به لیست

                    else:
                        print(f'{symbol} data is not available')

                # ایجاد دیتافریم فیکس با ایندکس‌های مطابق
                fixed_df = pd.DataFrame(index=index_list)
                fixed_df["FloatingShares"] = floating_share_list

                # ترکیب دیتافریم‌ها
                floating_share_df = floating_share_df.set_index("IranSymbol").join(fixed_df)

                downloaded_data = True
            else:
                print("Data has already been downloaded. Skipping further downloads.")

                break
        floating_share_df = floating_share_df.reset_index()
        return floating_share_df

    def fetch_iran_stock_key_stats(self):
        key_states = get_stats(base_path="hello", to_csv=False)
        key_states = pd.concat([key_states, pd.DataFrame({
            "Date": [datetime.datetime.today().date().strftime('%Y-%m-%d')] * len(key_states),
            "TimeFrame": [self.timeframe] * len(key_states)
        })], axis=1)

        key_states = key_states.rename(columns={"symbol": "IranSymbol", "index": "InsCode", })
        key_states["KeyStatesID"] = key_states["IranSymbol"] + "_" + key_states["Date"] + "_" + key_states["TimeFrame"]
        return key_states

    @staticmethod
    def fetch_intra_tse_market_watch():
        df_market_watch, df_order_book = fpy.Get_MarketWatch(
            save_excel=False,
            save_path='ProFinancialDss')

        df_market_watch = df_market_watch.reset_index()
        df_order_book = df_order_book.reset_index()
        df_order_book = df_order_book.drop(columns=['Day_LL', 'Day_UL'])
        df_order_book["Ticker"] = df_order_book["Ticker"].fillna(method='ffill')
        return df_market_watch, df_order_book

    def fetch_historical_order_book(self):
        dataframes = []
        for symbol in self.symbols_list:
            df_order_book_historical = fpy.Get_IntradayOB_History(
                stock=symbol,
                start_date=self.jalali_start_date,
                end_date=self.jalali_end_date,
                jalali_date=False,
                combined_datatime=False,
                show_progress=True)

            df_order_book_historical = df_order_book_historical.reset_index()
            df_order_book_historical["Date"] = df_order_book_historical["Date"].fillna(method='ffill')
            df_order_book_historical = df_order_book_historical.rename(columns={"Date":"GDate"})

            df_order_book_historical['GDate'] = pd.to_datetime(df_order_book_historical['GDate']).dt.strftime('%Y-%m-%d')
            df_order_book_historical['Time'] = pd.to_datetime(df_order_book_historical['Time'], format='%H:%M:%S').dt.strftime('%H:%M:%S')

            df_order_book_historical["IranSymbol"] = symbol

            dataframes.append(df_order_book_historical)

        all_df_order_book_historical = pd.concat(dataframes, ignore_index=True)

        return all_df_order_book_historical





# symbols = ["غدیس","فولاد"]
# # symbols = ["غدیس"]
# # # symbols = []
# iran = IranFinanceSource(symbols,"1d",'2023-09-01','2023-09-05')
# df = iran.fetch_csv_individual_corporate_files()
# print(df)

#
# df = iran.fetch_historical_order_book()
# df.to_excel("/home/shakour/shakour/Programming/Codes/ProFinancialDss/HistoricalOrderBook.xlsx")
# print(df.columns)

