import finpy_tse as fpy
import pandas as pd
import schedule
import time
from datetime import datetime, timedelta
import sqlite3


class MarketDataCollector:
    def __init__(self):
        """
        Constructor method. Initializes the MarketDataCollector class.
        """
        # Initialize a connection to the SQLite database
        self.conn = sqlite3.connect('IranInterDayData.db')

    def save_to_database(self):
        """
        Method to retrieve market data and save it to the database.
        """
        now = datetime.now()

        # Check if the current time is between 9:00 AM and 12:30 PM
        if now.time() >= datetime.strptime('09:00:00', '%H:%M:%S').time() and now.time() <= datetime.strptime('12:30:00', '%H:%M:%S').time():
            # Get the current date and format it as 'YYYY-MM-DD'
            date_str = now.strftime("%Y-%m-%d")

            # Create a table name based on the formatted date
            table_name = f"market_data_{date_str}"

            try:
                # Get market data
                df = fpy.Get_MarketWatch(
                    save_excel=False,
                    save_path='ProFinancialDss')[0]

                print(df)

                # Save the data to a table specific to the current date
                df.to_sql(table_name, self.conn, if_exists='append', index=True)
            except:
                print('An error occurred but the program will continue.')
                time.sleep(5)

    def run(self):
        """
        Main method to schedule data retrieval and database saving.
        """
        while True:
            try:
                self.save_to_database()
                time.sleep(5)
            except:
                print('An error occurred but the program will continue.')
                time.sleep(5)


if __name__ == "__main__":
    # Create an instance of the MarketDataCollector class
    collector = MarketDataCollector()

    # Start the data collection process
    collector.run()
