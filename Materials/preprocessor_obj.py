import pandas as pd
import numpy as np
from scipy.signal import detrend
from statsmodels.regression.linear_model import OLS
from statsmodels.stats.outliers_influence import OLSInfluence
from typing import List

class DataPreprocessor:
    """
    A Python class that provides additional data preprocessing methods.
    """

    @staticmethod
    def remove_outliers_threshold(data: pd.DataFrame, column: str, threshold: float) -> pd.DataFrame:
        """
        Removes outliers from a specific column based on a threshold value.

        :param data: The DataFrame to remove outliers from.
        :param column: The name of the column to remove outliers from.
        :param threshold: The threshold value to define outliers.
        :return: The DataFrame with outliers removed.
        """
        return data[data[column] <= threshold]

    @staticmethod
    def remove_outliers_rank(data: pd.DataFrame, column: str, percentile: float) -> pd.DataFrame:
        """
        Removes outliers from a specific column based on rank percentiles.

        :param data: The DataFrame to remove outliers from.
        :param column: The name of the column to remove outliers from.
        :param percentile: The percentile value (0-100) to define outliers based on ranks.
        :return: The DataFrame with outliers removed.
        """
        lower_bound = data[column].quantile(percentile / 100)
        upper_bound = data[column].quantile(1 - percentile / 100)
        return data[(data[column] >= lower_bound) & (data[column] <= upper_bound)]

    @staticmethod
    def remove_outliers_cooks_distance(data: pd.DataFrame, target_column: str, predictors: List[str], threshold: float) -> pd.DataFrame:
        """
        Removes outliers based on Cook's distance.

        :param data: The DataFrame to remove outliers from.
        :param target_column: The name of the target column used in the regression model.
        :param predictors: A list of column names representing the predictors in the regression model.
        :param threshold: The threshold value to define outliers based on Cook's distance.
        :return: The DataFrame with outliers removed.
        """
        x = data[predictors]
        y = data[target_column]

        model = OLSInfluence(OLS(y, x))
        cooks_d2 = model.cooks_distance[0]

        outliers = cooks_d2 > threshold
        return data[~outliers]

    @staticmethod
    def fill_missing_values(data: pd.DataFrame, method: str = 'ffill') -> pd.DataFrame:
        """
        Fills missing values in the DataFrame using a specified method.

        :param data: The DataFrame to fill missing values in.
        :param method: The method to use for filling missing values (default is forward fill).
                       Other options include 'bfill' (backward fill) or any valid Pandas fill method.
        :return: The DataFrame with missing values filled.
        """
        return data.fillna(method=method)

    @staticmethod
    def normalize_and_standardize_data(data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Normalizes and standardizes specified columns in the DataFrame.

        :param data: The DataFrame to be normalized and standardized.
        :param columns: A list of column names to be normalized and standardized.
        :return: The DataFrame with specified columns normalized and standardized.
        """
        for column in columns:
            col_mean = data[column].mean()
            col_std = data[column].std()
            data[f'Normalized{column}'] = (data[column] - col_mean) / col_std
        return data

    @staticmethod
    def log_transform_data(data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Performs logarithmic transformation on specified columns in the DataFrame.

        :param data: The DataFrame to be log-transformed.
        :param columns: A list of column names to be log-transformed.
        :return: The DataFrame with specified columns log-transformed.
        """
        for column in columns:
            data[column] = np.log(data[column])
        return data

    @staticmethod
    def resample(data: pd.DataFrame, frequency) -> pd.DataFrame:
        """
        Resamples the data to a specified frequency.

        :param data: The DataFrame to be resampled.
        :param frequency: The new frequency to resample the data to.
        :return: The resampled DataFrame.
        """
        return data.resample(frequency).mean()

    @staticmethod
    def handle_missing_data(data: pd.DataFrame, method='mean') -> pd.DataFrame:
        """
        Handles missing data in the DataFrame using a specified method.

        :param data: The DataFrame with missing values to be handled.
        :param method: The method to use for handling missing data (default is 'mean').
                       Options include 'mean', 'median', 'forward_fill', and 'backward_fill'.
        :return: The DataFrame with missing values handled according to the specified method.
        :raises ValueError: If an invalid method is provided.
        """
        if method == 'mean':
            filled_data = data.fillna(data.mean())
        elif method == 'median':
            filled_data = data.fillna(data.median())
        elif method == 'forward_fill':
            filled_data = data.ffill()
        elif method == 'backward_fill':
            filled_data = data.bfill()
        else:
            raise ValueError("Invalid method. Supported methods are: mean, median, forward_fill, backward_fill.")
        return filled_data

    @staticmethod
    def detrend(data: pd.DataFrame, method='linear') -> pd.Series:
        """
        Detrends the data using a specified method.

        :param data: The DataFrame to be detrended.
        :param method: The detrending method to use (default is 'linear').
                       Options include 'linear' and other valid options supported by scipy.signal.detrend().
        :return: The detrended data.
        """
        detrended_data = detrend(data, type=method)
        return pd.Series(detrended_data, index=data.index)

    @staticmethod
    def denormalize_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        Denormalizes the data back to its original scale.

        :param data: The DataFrame to be denormalized.
        :return: The denormalized DataFrame.
        """
        return data * data.std() + data.mean()

    @staticmethod
    def destandardize_data(data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Destandardizes specified columns back to their original scale.

        :param data: The DataFrame to be destandardized.
        :param columns: A list of column names to be destandardized.
        :return: The destandardized DataFrame.
        """
        for column in columns:
            col_mean = data[column].mean()
            col_std = data[column].std()
            data[column] = data[column] * col_std + col_mean
        return data

