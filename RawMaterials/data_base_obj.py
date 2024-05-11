# developed by: Shakour Alishahi
# ======================================================================================================================
# ######################################################################################################################
# import external libraries
# ----------------------------------------------------------------------------------------------------------------------
import sqlite3
import json
import pandas as pd
from jdatetime import datetime as jdatetime
import datetime
import os
import shutil
import numpy as np
import time
import functools

from datetime import timedelta

# ======================================================================================================================
# ######################################################################################################################
# import internal libraries
# ----------------------------------------------------------------------------------------------------------------------

# ======================================================================================================================
# ######################################################################################################################
# Database call
# ----------------------------------------------------------------------------------------------------------------------


# ======================================================================================================================
# ######################################################################################################################
class DataHelper:
    """
    A utility class for data manipulation and operations.

    This class provides various functions for tasks such as data mapping, loading data from different sources,
    converting Gregorian dates to Jalali (Persian) dates, saving and loading data in JSON format, creating pivot tables,
    and filtering DataFrames based on user-defined conditions.

    Usage:
        data_helper = DataHelper()
        result = data_helper.some_function()

    Attributes:
        None

    Methods:
        - mapping_columns(main_df, data_df, pivot_column, Target_column): Map columns in the main DataFrame based on
          a mapping dictionary from another DataFrame.
        - load_table_as_dataframe(table_name, conn, column_check_duplicate): Load data from a database table into a
          DataFrame and remove duplicate rows.
        - gregorian_to_jalali(gregorian_date): Convert a Gregorian date to Jalali (Persian) date.
        - save_dict_to_json(data, filename): Save a dictionary to a JSON file.
        - load_json_to_dict(filename): Load a JSON file and return its contents as a dictionary.
        - load_excel_to_json(excel_filename, sheet_name, json_filename): Load Excel data from a specified sheet and save
          it as JSON.
        - dataframe_to_json(dataframe, json_filename): Save a DataFrame as a JSON file.
        - json_to_dataframe(json_filename): Load a JSON file and convert it to a DataFrame.
        - filter_dataframe(dataframe, filter_conditions): Filter a DataFrame based on a list of filter conditions.
        - create_pivot_table(dataframe, pivot_columns, values_columns, aggfunc='sum'): Create a Pivot table based on
          specified columns and aggregation function.
        - rename_date_excel_files(input_path): Rename Excel files in the specified directory and its subdirectories based on
          date extracted from the filename.

    """

    def __init__(self):
        self.project_path = '/home/shakour/shakour/Programming/Codes/GitStudy/MarketMakerReporter'
        self.g_today, self.j_today = self.today_date_as_string()

    @staticmethod
    def mapping_columns(main_df, data_df, pivot_column, target_column, drop_pivot_column=False):
        """
        Map columns in the main DataFrame based on a mapping dictionary from another DataFrame.

        Args:
            main_df (pd.DataFrame): The main DataFrame to be updated.
            data_df (pd.DataFrame): The DataFrame containing the mapping information.
            pivot_column (str): The column in the main DataFrame used as the key for mapping.
            target_column (str): The column in the mapping DataFrame to be mapped to the main DataFrame.
            drop_pivot_column (bool, optional): Whether to drop the pivot column from the main DataFrame.
                                              Default is True.

        Returns:
            pd.DataFrame: The main DataFrame with mapped values.
        """
        mapping_dict = data_df.set_index(pivot_column)[target_column].to_dict()
        # Apply the mapping to create SectorID column in df
        main_df[target_column] = main_df[pivot_column].map(mapping_dict)
        if drop_pivot_column:
            main_df.drop(columns=[pivot_column], inplace=True)
        return main_df

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def load_table_as_dataframe(table_name, conn, column_check_duplicate):
        """
        Load data from a database table into a DataFrame and remove duplicate rows.

        Args:
            table_name (str): The name of the database table to load data from.
            conn: The database connection object.
            column_check_duplicate (str): The column to check for duplicate rows.

        Returns:
            pd.DataFrame: The loaded DataFrame.
        """
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        df = df.drop_duplicates(subset=[column_check_duplicate])
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def gregorian_to_jalali(gregorian_date):
        """
        Convert a Gregorian date to Jalali (Persian) date.

        Args:
            gregorian_date (str): The input Gregorian date in 'YYYY-MM-DD' format.

        Returns:
            str: The converted Jalali date in 'YYYY-MM-DD' format.
        """
        gregorian_datetime = datetime.datetime.strptime(gregorian_date, "%Y-%m-%d")
        gregorian_date = gregorian_datetime.date()
        jalali_date = jdatetime.fromgregorian(date=gregorian_date).strftime('%Y-%m-%d')
        return jalali_date

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_to_gregorian(jalali_date):
        """
        Convert a Jalali (Persian) date to Gregorian date.

        Args:
            jalali_date (str): The input Jalali date in 'YYYY-MM-DD' format.

        Returns:
            str: The converted Gregorian date in 'YYYY-MM-DD' format.
        """
        jalali_datetime = jdatetime.strptime(jalali_date, "%Y-%m-%d")
        gregorian_date = jalali_datetime.togregorian().strftime('%Y-%m-%d')
        return gregorian_date

    # ------------------------------------------------------------------------------------------------------------------

    def add_converted_date(self, df, conversion_type, source_column, target_column):
        """
        Converts dates in a DataFrame from Jalali to Gregorian or vice versa.

        Args:
            df (pandas.DataFrame): The input DataFrame.
            conversion_type (str): The type of conversion to perform.
                Should be either 'jalali_to_gregorian' or 'gregorian_to_jalali'.
            source_column (str): The name of the source column containing dates.
            target_column (str): The name of the column to store the converted dates.

        Returns:
            pandas.DataFrame: The DataFrame with the converted dates.

        Raises:
            ValueError: If an invalid conversion type is provided.

        Example:
            converter = DateConverter()
            df = converter.add_converted_date(df, 'jalali_to_gregorian', 'jalali_dates', 'gregorian_dates')
        """
        if conversion_type == 'jalali_to_gregorian':
            df[target_column] = df[source_column].apply(self.jalali_to_gregorian)
        elif conversion_type == 'gregorian_to_jalali':
            df[target_column] = df[source_column].apply(self.gregorian_to_jalali)
        else:
            raise ValueError("Invalid conversion type. Please choose 'jalali_to_gregorian' or 'gregorian_to_jalali'.")

        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def gregorian_datetime_to_jalali(gregorian_datetime):
        """
        Converts a Gregorian date and time to Jalali date and time.

        Args:
        gregorian_datetime (datetime): The Gregorian date and time.

        Returns:
        str: Date and time in Jalali format, formatted as 'YYYY-MM-DD HH:MM:SS'.
        """
        jalali_date = jdatetime.fromgregorian(date=gregorian_datetime.date(),
                                              time=gregorian_datetime.time()).strftime('%Y-%m-%d %H:%M:%S')
        return jalali_date

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_datetime_to_gregorian(jalali_datetime_str):
        """
        Converts a Jalali date and time to Gregorian date and time.

        Args:
        jalali_datetime_str (str): Date and time in Jalali format, formatted as 'YYYY-MM-DD HH:MM:SS'.

        Returns:
        str: Date and time in Gregorian format, formatted as 'YYYY-MM-DD HH:MM:SS'.
        """
        jalali_datetime = jdatetime.strptime(jalali_datetime_str, '%Y-%m-%d %H:%M:%S')
        gregorian_date = jalali_datetime.togregorian().strftime('%Y-%m-%d %H:%M:%S')
        return gregorian_date

    # ------------------------------------------------------------------------------------------------------------------

    def add_converted_datetime(self, df, conversion_type, source_column, target_column):
        """
        Adds a new column with converted date and time values to the DataFrame.

        Args:
        df (pandas.DataFrame): The DataFrame containing the date and time values.
        conversion_type (str): The type of conversion to perform. Options: 'jalali_to_gregorian' or 'gregorian_to_jalali'.
        source_column (str): The name of the column containing the original date and time values.
        target_column (str): The name of the new column where the converted values will be stored.

        Returns:
        pandas.DataFrame: The DataFrame with the additional converted date and time column.
        """
        if conversion_type == 'jalali_to_gregorian':
            df[target_column] = df[source_column].apply(self.jalali_datetime_to_gregorian)
        elif conversion_type == 'gregorian_to_jalali':
            df[target_column] = df[source_column].apply(self.gregorian_datetime_to_jalali)
        else:
            print("Invalid conversion type. Please choose 'jalali_to_gregorian' or 'gregorian_to_jalali'.")
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def string_to_datetime(date_str):
        try:
            date_obj = datetime.strptime(date_str,
                                         '%Y-%m-%d %H:%M:%S')  # فرض کردیم که فرمت ورودی 'YYYY-MM-DD HH:MM:SS' است
            return date_obj
        except ValueError:
            print("Invalid date format. Please provide date in the format 'YYYY-MM-DD HH:MM:SS'")
            return None

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def save_dict_to_json(data: dict, filename: str):
        """
        Save a dictionary to a JSON file.

        Args:
            data (dict): The dictionary to be saved.
            filename (str): The path to the JSON file.

        Returns:
            None
        """
        with open(filename, 'w') as json_file:
            json.dump(data, json_file, indent=4)
            print(f"Data saved to {filename}")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def load_json_to_dict(filename: str) -> dict:
        """
        Load a JSON file and return its contents as a dictionary.

        Args:
            filename (str): The path to the JSON file.

        Returns:
            dict: The loaded dictionary.
        """
        with open(filename, 'r') as json_file:
            json_data = json.load(json_file)
            return json_data

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def load_excel_to_json(excel_filename: str, sheet_name: str, json_filename: str):
        """
        Load Excel data from a specified sheet and save it as JSON.

        Args:
            excel_filename (str): The path to the Excel file.
            sheet_name (str): The name of the sheet to load data from.
            json_filename (str): The path to the JSON file to be created.

        Returns:
            None
        """
        excel_data = pd.read_excel(excel_filename, sheet_name=sheet_name)
        json_data = excel_data.to_dict(orient='records')

        with open(json_filename, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            print(f"Data from '{sheet_name}' sheet saved to {json_filename}")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def dataframe_to_json(dataframe, json_filename):
        """
        Save a DataFrame as a JSON file.

        Args:
            dataframe (pd.DataFrame): The DataFrame to be saved as JSON.
            json_filename (str): The path to the JSON file where the DataFrame will be saved.

        Returns:
            None
        """
        json_data = dataframe.to_json(orient='records', indent=4)
        with open(json_filename, 'w') as json_file:
            json_file.write(json_data)
            print(f"DataFrame saved as JSON to {json_filename}")
    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def json_to_dataframe(json_filename):
        """
        Load a JSON file and convert it to a DataFrame.

        Args:
            json_filename (str): The path to the JSON file to be loaded.

        Returns:
            pd.DataFrame: The DataFrame created from the JSON data.
        """
        with open(json_filename, 'r') as json_file:
            json_data = json.load(json_file)
        dataframe = pd.DataFrame.from_dict(json_data)
        return dataframe

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def create_filter_dataframe(dataframe, filter_conditions):
        """
        Filter a DataFrame based on a list of filter conditions.

        Args:
            dataframe (pd.DataFrame): The DataFrame to be filtered.
            filter_conditions (list): A list of dictionaries, each containing filter conditions.
                Each dictionary should have the following keys:
                - 'column_name': The name of the column to filter on.
                - 'value': The value to compare with.
                - 'operator' (optional): The comparison operator (e.g., '==', '!=', '>', '<', etc.).

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        filtered_df = dataframe.copy()

        for condition in filter_conditions:
            column_name = condition.get('column_name')
            value = condition.get('value')
            operator = condition.get('operator', '==')  # Default operator is '=='

            if operator == '==':
                filtered_df = filtered_df[filtered_df[column_name] == value]
            elif operator == '!=':
                filtered_df = filtered_df[filtered_df[column_name] != value]
            elif operator == '>':
                filtered_df = filtered_df[filtered_df[column_name] > value]
            elif operator == '<':
                filtered_df = filtered_df[filtered_df[column_name] < value]
            elif operator == '<=':
                filtered_df = filtered_df[filtered_df[column_name] <= value]
            elif operator == '>=':
                filtered_df = filtered_df[filtered_df[column_name] >= value]
            elif operator == '<>':
                upper_value = condition.get('value1')
                lower_value = condition.get('value2')
                filtered_df = filtered_df[(filtered_df[column_name] >= upper_value) & (filtered_df[column_name] <= lower_value)]

            # Add more operators as needed

        return filtered_df
# Usage **************************************************
# # تعریف شرایط فیلترینگ به صورت یک لیست از دیکشنری‌ها
# filter_conditions = [
#     {'column_name': 'Age', 'value': 30, 'operator': '>'},
#     {'column_name': 'Gender', 'value': 'Male'},
#     {'column_name': 'Salary', 'value': 50000, 'operator': '<'},
# ]
#
# # فیلتر کردن DataFrame با استفاده از شرایط فیلترینگ
# filtered_df = data_helper.create_filter_dataframe(dataframe, filter_conditions)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def create_pivot_table(dataframe, pivot_columns, values_columns, aggfunc='sum'):
        """
        Create a Pivot table based on specified columns.

        Args:
            dataframe (pd.DataFrame): The DataFrame to create the Pivot table from.
            pivot_columns (list): A list of column names for Pivot.
            values_columns (list): A list of column names for Pivot values.
            aggfunc (str): The aggregation function (e.g., 'sum', 'mean', 'count', etc.).

        Returns:
            pd.DataFrame: The Pivot table.
        """
        pivot_table = dataframe.pivot_table(index=pivot_columns, values=values_columns, aggfunc=aggfunc)
        return pivot_table

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def rename_date_excel_files(input_path):
        """
        Rename Excel files based on the date extracted from their filenames in the specified directory and its subdirectories.

        Args:
            input_path (str): The directory path containing Excel files to be renamed.

        This function identifies Excel files within the specified directory and its subdirectories, extracts the date
        from their filenames, and renames them to a new format. The date is expected to be in the filename format
        "yyyyMMdd" (e.g., "20230517" for May 17, 2023), and the function renames the files to the format "yyMMdd-dd-MM-yyyy"
        (e.g., "230517-17-05-2023").

        Returns:
            None
        """
        excel_files = [os.path.join(root, file) for root, dirs, files in os.walk(input_path) for file in files if
                       file.endswith(".xlsx")]

        for excel_file in excel_files:
            date_str = excel_file.split("_")[-1].split(".")[0]

            # Check if the filename adheres to the expected date-based format
            if len(date_str) == 8 and date_str.isdigit():
                formatted_date = f"{date_str[:2]}{date_str[2:4]}-{date_str[4:6]}-{date_str[6:8]}"
                new_file_name = excel_file.replace(date_str, formatted_date)
                os.rename(excel_file, new_file_name)
                print(f"File renamed: {excel_file} to {new_file_name}")
            else:
                print(f"File does not conform to the expected format and was not renamed: {excel_file}")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def merge_dataframes(df_list, common_column):
        combined_df = df_list[0]
        for i in range(1, len(df_list)):
            combined_df = combined_df.merge(df_list[i], on=common_column)
        return combined_df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def split_datetime(df, column_name, date_format='%Y-%m-%d', time_format='%H:%M:%S', column_date_name='Date', column_time_name='Time', keep_original=False):
        # تبدیل ستون به تاریخ
        df[column_name] = pd.to_datetime(df[column_name])

        # استخراج تاریخ و زمان به صورت رشته
        df[column_date_name] = df[column_name].dt.strftime(date_format)
        df[column_time_name] = df[column_name].dt.strftime(time_format)

        # حذف ستون اصلی اگر مشخص شده باشد
        if not keep_original:
            df.drop(column_name, axis=1, inplace=True)

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def split_str_datetime(datetime_str, default_spliter_date, costume_spliter_date):
        try:
            split_values = datetime_str.split(' ')
            split_date = split_values[0]  # اینجا تاریخ قرار می‌گیرد
            split_time = split_values[1]  # اینجا زمان قرار می‌گیرد
            updated_date = split_date.replace(default_spliter_date, costume_spliter_date)
        except IndexError:
            updated_date = datetime_str
            split_time = "زمان نامشخص"

        return updated_date, split_time

    # ------------------------------------------------------------------------------------------------------------------

    def convert_datetime_column(self, df, datetime_column_name, date_column_name, time_column_name,
                                defult_spliter_date='/',
                                costume_spliter_date='-', drop_original_column=True, drop_time_column=True):
        df[date_column_name], df[time_column_name] = zip(
            *df[datetime_column_name].apply(
                lambda x: DataHelper.split_str_datetime(x, defult_spliter_date, costume_spliter_date)))
        if drop_original_column:
            df.drop(datetime_column_name, axis=1, inplace=True)

        if drop_time_column:
            df.drop(time_column_name, axis=1, inplace=True)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def round_time_column_to_strings(df, column_name, n):
        def round_time_to_nearest_n_seconds(time_str):
            time_format = '%H:%M:%S'
            time_obj = datetime.datetime.strptime(time_str, time_format)
            rounded_seconds = round(time_obj.second / n) * n
            rounded_time = time_obj.replace(second=int(rounded_seconds))
            return rounded_time.strftime(time_format)

        df[column_name] = df[column_name].apply(round_time_to_nearest_n_seconds)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def calculate_seconds_avg(df, datetime_column, seconds):
        """
        Calculate the average values within specified time intervals.

        Parameters:
            df (pandas.DataFrame): The DataFrame containing the data.
            datetime_column (str): The name of the column containing datetime values.
            seconds (int): The number of seconds to use as the time interval.

        Returns:
            pandas.DataFrame: A DataFrame containing the calculated average values.

        Example:
            df = pd.DataFrame({
                "ID": ["ID1", "ID2", "ID3"],
                "Datetime": ["2023-09-02 09:00:34", "2023-09-02 09:00:36", "2023-09-02 09:00:38"],
                "Value": [1, 2, 3]
            })
            datetime_column = "Datetime"
            seconds = 5
            result_df = calculate_seconds_avg(df, datetime_column, seconds)
            print(result_df)
        """
        # Convert datetime to a format suitable for calculation
        df[datetime_column] = pd.to_datetime(df[datetime_column])

        # Group by specified time intervals (in seconds)
        df['Time_Group'] = df[datetime_column].dt.floor(f'{seconds}s')

        # Calculate the average within each group
        average_values = df.groupby('Time_Group').mean()

        return average_values

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def today_date_as_string():
        # Get today's date in Gregorian and Jalali calendars
        current_gregorian_date = datetime.datetime.now().date()
        current_jalali_date = jdatetime.now().strftime('%Y-%m-%d')

        return str(current_gregorian_date), current_jalali_date

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_year(jalali_date):
        jalali_date = jdatetime.fromisoformat(jalali_date)
        return jalali_date.year

    # ------------------------------------------------------------------------------------------------------------------

    def add_jalali_year_column(self, df):
        df['JYear'] = df['JDate'].apply(self.jalali_year)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_to_month_year(jalali_date):
        jalali_date = jdatetime.fromisoformat(jalali_date)
        return f"{jalali_date.strftime('%B')} {jalali_date.year}"

    # ------------------------------------------------------------------------------------------------------------------

    def add_month_year_column(self, df):
        df['JMonthYear'] = df['JDate'].apply(self.jalali_to_month_year)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_to_month_year(jalali_date):
        jalali_date = jdatetime.fromisoformat(jalali_date)
        return f"{jalali_date.strftime('%B')}"

    # ------------------------------------------------------------------------------------------------------------------

    def add_jalali_month_year_column(self, df):
        df['JMonthYear'] = df['JDate'].apply(self.jalali_to_month_year)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_month_number(jalali_date):
        jalali_date = jdatetime.fromisoformat(jalali_date)
        return jalali_date.month

    # ------------------------------------------------------------------------------------------------------------------

    def add_jalali_month_number_column(self, df):
        df['JMonthNumber'] = df['JDate'].apply(self.jalali_month_number)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_day_of_month(jalali_date):
        jalali_date = jdatetime.fromisoformat(jalali_date)
        return jalali_date.day

    # ------------------------------------------------------------------------------------------------------------------

    def add_jalali_day_of_month_column(self, df):
        df['JDayOfMonth'] = df['JDate'].apply(self.jalali_day_of_month)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_week_of_year(jalali_date):
        jalali_date = jdatetime.fromisoformat(jalali_date)
        year, week, _ = jalali_date.isocalendar()
        return f"{week:02d}"

    # ------------------------------------------------------------------------------------------------------------------

    def add_jalali_week_number_column(self, df):
        df['JWeekNumber'] = df['JDate'].apply(self.jalali_week_of_year)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_season(jalali_date):
        jalali_date = jdatetime.fromisoformat(jalali_date)
        month = jalali_date.month
        if month >= 1 and month <= 3:
            return 'Spring'
        elif month >= 4 and month <= 6:
            return 'Summer'
        elif month >= 7 and month <= 9:
            return 'Autumn'
        else:
            return 'Winter'

    # ------------------------------------------------------------------------------------------------------------------

    def add_jalali_season_column(self, df):
        df['JSeason'] = df['JDate'].apply(self.jalali_season)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_half_year(jalali_date):
        jalali_date = jdatetime.fromisoformat(jalali_date)
        month = jalali_date.month
        if month >= 1 and month <= 6:
            return 'First Half'
        else:
            return 'Second Half'

    # ------------------------------------------------------------------------------------------------------------------

    def add_jalali_half_year_column(self, df):
        df['JHalfYear'] = df['JDate'].apply(self.jalali_half_year)
        return df

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def gregorian_year(gregorian_date):
        gregorian_date = datetime.datetime.fromisoformat(gregorian_date)
        return gregorian_date.year

    def add_gregorian_year_column(self, df):
        df['GYear'] = df['GDate'].apply(self.gregorian_year)
        return df

    @staticmethod
    def gregorian_half_year(gregorian_date):
        gregorian_date = datetime.datetime.fromisoformat(gregorian_date)
        month = gregorian_date.month
        if month >= 1 and month <= 6:
            return 'First Half'
        else:
            return 'Second Half'

    # ------------------------------------------------------------------------------------------------------------------

    def add_gregorian_half_year_column(self, df):
        df['GHalfYear'] = df['GDate'].apply(self.gregorian_half_year)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def gregorian_to_month_year(gregorian_date):
        gregorian_date = datetime.datetime.fromisoformat(gregorian_date)
        return f"{gregorian_date.strftime('%B')}"

    # ------------------------------------------------------------------------------------------------------------------

    def add_gregorian_month_year_column(self, df):
        df['GMonthYear'] = df['GDate'].apply(self.gregorian_to_month_year)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def gregorian_month_number(gregorian_date):
        gregorian_date = datetime.datetime.fromisoformat(gregorian_date)
        return gregorian_date.month

    # ------------------------------------------------------------------------------------------------------------------

    def add_gregorian_month_number_column(self, df):
        df['GMonthNumber'] = df['GDate'].apply(self.gregorian_month_number)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def gregorian_day_of_month(gregorian_date):
        gregorian_date = datetime.datetime.fromisoformat(gregorian_date)
        return gregorian_date.day

    # ------------------------------------------------------------------------------------------------------------------

    def add_gregorian_day_of_month_column(self, df):
        df['GDayOfMonth'] = df['GDate'].apply(self.gregorian_day_of_month)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def gregorian_week_of_year(gregorian_date):
        gregorian_date = datetime.datetime.fromisoformat(gregorian_date)
        return gregorian_date.strftime('%U')

    # ------------------------------------------------------------------------------------------------------------------

    def add_gregorian_week_number_column(self, df):
        df['GWeekNumber'] = df['GDate'].apply(self.gregorian_week_of_year)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def get_last_date(df, gregorian_column_name):

        """
        Finds the last date in the given column.

        Args:
            df (pandas.DataFrame): The DataFrame containing the column to be searched.
            gregorian_column_name (str): The name of the column to be searched.

        Returns:
            str: The last date in the given column, formatted as YYYY-MM-DD.
        """

        # Convert the column to a datetime object
        df[gregorian_column_name] = pd.to_datetime(df[gregorian_column_name], format='%Y-%m-%d')

        # Find the last date
        last_date = max(df[gregorian_column_name])

        return last_date.strftime('%Y-%m-%d')

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def move_files(source_folder, destination_folder, delete_original=False):
        # Get the list of files in the source folder
        file_list = os.listdir(source_folder)

        # Loop through the files
        for file_name in file_list:
            source_file = os.path.join(source_folder, file_name)
            destination_file = os.path.join(destination_folder, file_name)

            # Copy the file from source to destination
            shutil.copy(source_file, destination_file)

            # Delete the original file if delete_original=True
            if delete_original:
                os.remove(source_file)

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def jalali_date_n_days_ago(date_str, n):
        # تبدیل تاریخ جلالی به میلادی
        jalali_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        # افزودن n روز به تاریخ جلالی
        new_date = jalali_date - timedelta(days=n)
        # تبدیل تاریخ به رشته و بازگردانی آن
        return new_date.strftime("%Y-%m-%d")

    # ------------------------------------------------------------------------------------------------------------------

    def extraction_date_list(self, start_report, end_report):
        conn_basic_database = sqlite3.connect(f"{self.project_path}/Warehouse/BasicDataBase.db")

        # Read data from DateTbl table
        df_date = self.load_table_as_dataframe("DateTbl", conn_basic_database, "GDate")

        # filter date
        # --- Filtering conditions
        filter_conditions = [
            {'column_name': 'JDate', 'value': start_report, 'operator': '>='},
            {'column_name': 'JDate', 'value': end_report, 'operator': '<='},
        ]

        df_date_filtered = self.create_filter_dataframe(df_date, filter_conditions=filter_conditions)
        df_date_filtered = df_date_filtered.dropna()

        jdate_list = list(df_date_filtered["JDate"])

        conn_basic_database.close()
        print(jdate_list)
        return jdate_list

    # ------------------------------------------------------------------------------------------------------------------

    # Todo This function is newly written and should be replaced in all functions and classes. 1402-07-14 -> 1402/08/30
    def build_table_dataframe(self, db_name, table_name, column_check_duplicate):
        conn = sqlite3.connect(f'{self.project_path}/Warehouse/{db_name}')
        # Read data from DateTbl table
        table_dataframe = self.load_table_as_dataframe(table_name, conn, column_check_duplicate)
        return table_dataframe

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def read_csv_to_dataframe(path, file_name):
        """
        This function takes a CSV file name and returns it as a DataFrame.

        Args:
        file_name (str): The CSV file name without extension.

        Returns:
        pandas.DataFrame: DataFrame containing the data from the CSV file.
        """
        try:
            # Adding the .csv extension to the file name
            file_path = f"{path}/{file_name}.csv"

            # Reading the CSV file and converting it to a DataFrame
            df = pd.read_csv(file_path)

            return df
        except Exception as e:
            print(f"Error while reading the file: {e}")
            return None

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def split_dataframe(df, chunk_size):
        """
        Splits a given DataFrame into smaller chunks.

        Parameters:
            df (pd.DataFrame): The original DataFrame.
            chunk_size (int): The size of each chunk.

        Returns:
            List of pd.DataFrame: A list of split DataFrames.

        Example Usage:
            Assuming 'df' is the original DataFrame and you want to split it into chunks of size 100.
            chunk_size = 100
            split_dfs = split_dataframe(df, chunk_size)
        """
        # Calculate the number of chunks needed
        num_chunks = len(df) // chunk_size
        if len(df) % chunk_size != 0:
            num_chunks += 1

        # Split the DataFrame into chunks
        split_dfs = [df[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]

        return split_dfs

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def vlookup(df1, df2, on_column, lookup_column, new_column_name):
        """
        این تابع یک عملیات مشابه به VLOOKUP در اکسل بین دو دیتافریم انجام می‌دهد.

        :param df1: دیتافریم اصلی
        :param df2: دیتافریم که اطلاعات از آن دریافت می‌شود
        :param on_column: نام ستونی در df1 که برای ادغام استفاده می‌شود
        :param lookup_column: نام ستونی در df2 که اطلاعات از آن دریافت می‌شود
        :param new_column_name: نام ستون جدید که اطلاعات در آن ذخیره می‌شود
        :return: دیتافریم جدید با ستون جدید
        """
        merged_df = pd.merge(df1, df2, how='left', left_on=on_column, right_on=lookup_column)
        merged_df = merged_df.drop(columns=[lookup_column])
        merged_df = merged_df.rename(columns={on_column: new_column_name})
        return merged_df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def create_summary_col_dataframe(df, columns_to_sum):
        """
        Create a new dataframe with one row containing the sum of specified columns in the original dataframe.

        Args:
            df (pd.DataFrame): The original dataframe.
            columns_to_sum (list): List of column names to calculate the sum for.

        Returns:
            pd.DataFrame: The new dataframe with column names prefixed with "Sum".
        """
        # Create a new dataframe with one row containing the sum of specified columns in the original dataframe
        sum_df = pd.DataFrame(df[columns_to_sum].sum()).T

        # Add prefix "Sum" to column names
        sum_df.columns = ["Sum" + col for col in sum_df.columns]

        return sum_df

    # ------------------------------------------------------------------------------------------------------------------

    def build_filter_between_two_value_df(self, df, column_name, value1, value2):
        filter_conditions = [
            {'column_name': column_name, 'value': value1, 'operator': '>='},
            {'column_name': column_name, 'value': value2, 'operator': '<='}
        ]

        filter_between_two_value_df = self.create_filter_dataframe(df, filter_conditions)
        filter_between_two_value_df = filter_between_two_value_df.reset_index()
        filter_between_two_value_df = filter_between_two_value_df.drop(columns=['index'])

        return filter_between_two_value_df

    # ------------------------------------------------------------------------------------------------------------------

    def build_filter_by_column_value_df(self, df, column_name, column_value):
        filter_conditions = [
            {'column_name': column_name, 'value': column_value, 'operator': '=='}

        ]

        filter_by_column_value_df = self.create_filter_dataframe(df, filter_conditions)
        filter_by_column_value_df = filter_by_column_value_df.reset_index()
        filter_by_column_value_df = filter_by_column_value_df.drop(columns=['index'])

        return filter_by_column_value_df

    # ------------------------------------------------------------------------------------------------------------------

    def build_filter_specially_raw_df(self, df, column1_name, column2_name, value1, value2):
        filter_conditions = [
            {'column_name': column1_name, 'value': value1, 'operator': '=='},
            {'column_name': column2_name, 'value': value2, 'operator': '=='}
        ]

        filter_specially_raw_df = self.create_filter_dataframe(df, filter_conditions)
        filter_specially_raw_df = filter_specially_raw_df.reset_index()
        filter_specially_raw_df = filter_specially_raw_df.drop(columns=['index'])

        return filter_specially_raw_df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def divide_columns_by_number(df, columns, divisor):
        for column in columns:
            df[column] = np.round(df[column] / divisor, 2)  # گرد کردن به دو رقم اعشار
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def remove_spaces(text):
        words = text.split()
        result = ''.join(words)
        return result

    # ------------------------------------------------------------------------------------------------------------------

    def remove_spaces_from_column(self, df, input_column, output_column):
        df[output_column] = df[input_column].apply(self.remove_spaces)
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def translate_values(original_df, columns_to_translate, word_dict_df):
        translated_df = original_df.copy()

        for col in original_df.columns:
            if col in columns_to_translate:
                translated_values = []
                for value in original_df[col]:
                    translated_value = word_dict_df.loc[word_dict_df['SystemWord'] == value, 'PersianWord']
                    if not translated_value.empty:
                        translated_values.append(translated_value.values[0])
                    else:
                        translated_values.append(value)
                translated_df[col] = translated_values

        return translated_df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def check_table_existence(database, table_name):
        conn = sqlite3.connect(database)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()
        conn.close()
        if result:
            return True
        else:
            return False

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def move_column_to_first(df, column_name):
        # انتخاب ستون مورد نظر برای جابجایی
        selected_column = df[column_name]

        # حذف ستون انتخاب شده از دیتافریم
        df = df.drop(column_name, axis=1)

        # اضافه کردن ستون در موقعیت اول
        df.insert(0, column_name, selected_column)

        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def fillna_groupby_previous(df, group_cols, fill_cols):
        # پر کردن مقادیر خالی با مقدار قبلی در ستون‌های مورد نظر
        df[fill_cols] = df.groupby(group_cols)[fill_cols].fillna(method='ffill')
        return df

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def calculate_execution_time(func):
        """
        A decorator function to calculate the execution time of a given function.

        Args:
        - func: The function for which the execution time needs to be measured.

        Returns:
        - The result of the input function.
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Execution time of '{func.__name__}': {execution_time} seconds")
            return result

        return wrapper

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def fill_empty_with_record_value(dataframe, group_column, fill_column):
        # Create a new column 'valid_values' to store the first valid values for each group
        dataframe['valid_values'] = dataframe.groupby(group_column)[fill_column].transform(
            lambda x: x.loc[x.first_valid_index()])

        # Fill empty records in 'fill_column' with values from 'valid_values' column
        dataframe[fill_column].fillna(dataframe['valid_values'], inplace=True)

        # Drop the temporary 'valid_values' column
        dataframe.drop(columns=['valid_values'], inplace=True)

        return dataframe

    # ------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def compare_dataframe_with_table(dataframe, table_name, db_name):
        conn = sqlite3.connect(db_name)

        try:
            # Read the existing table from the database
            query = f"SELECT * FROM {table_name}"
            existing_table = pd.read_sql(query, conn)

            # Check for identical rows between the DataFrame and the existing table
            merged_df = pd.merge(dataframe, existing_table, indicator=True, how='outer')
            unique_rows = merged_df[merged_df['_merge'] == 'left_only']

            if len(unique_rows) > 0:
                # If there are unique rows in the DataFrame, add them to the database
                unique_rows.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"Unique rows added to the {table_name} table.")
            else:
                print("No unique rows found to add to the table.")

        except sqlite3.Error as e:
            print("Error:", e)

        conn.close()

    # ------------------------------------------------------------------------------------------------------------------

    # def convert_db_to_json(db_path, table_name, json_file_path):
    #     # اتصال به دیتابیس
    #     connection = sqlite3.connect(db_path)
    #     cursor = connection.cursor()
    #
    #     # دریافت اطلاعات از جدول
    #     cursor.execute(f"SELECT * FROM {table_name}")
    #     rows = cursor.fetchall()
    #
    #     # تبدیل اطلاعات به فرمت JSON
    #     data = []
    #     for row in rows:
    #         row_dict = {}
    #         for i, column in enumerate(cursor.description):
    #             row_dict[column[0]] = row[i]
    #         data.append(row_dict)
    #
    #     # نوشتن اطلاعات به فایل JSON
    #     with open(json_file_path, 'w') as json_file:
    #         json.dump(data, json_file, indent=4)
    #
    #     # بستن اتصال
    #     cursor.close()
    #     connection.close()
    #
    # # مثال استفاده از تابع
    # convert_db_to_json(
    #     "/home/shakour/shakour/Programming/Codes/GitStudy/MarketMakerReporter/Warehouse/BasicDataBase.db",
    #     "DateTbl",
    #     "Mines/DateTbl.json")

# ====================================================================================================================
# helper = DataHelper()
# result = helper.jalali_date_n_days_ago("1402-08-20", 365)
# print(result)
