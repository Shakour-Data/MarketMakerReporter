import pandas as pd

class PortfolioManagementCalculator:
    def __init__(self):
        pass

    @staticmethod
    def calculate_returns(dataframe, price_column='Price'):
        # Calculate the daily returns and add a new column to the dataframe
        dataframe[f'{price_column}Returns'] = (dataframe[price_column] - dataframe[price_column].shift(1)) / dataframe[
            price_column].shift(1) * 100

        # Replace NaN in the first row with a specified value (e.g., 0)
        dataframe.at[dataframe.index[0], f'{price_column}Returns'] = 0

        return dataframe



