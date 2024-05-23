# data_cleaning.py

import pandas as pd

class DataCleaning:
    def __init__(self, data):
        self.data = data

    def fill_missing_values(self):
        """Fill missing values in the 'keyword' column with 'unknown'."""
        self.data['keyword'] = self.data['keyword'].fillna('unknown')
        return self

    def convert_to_numeric(self):
        """Convert 'views' column to numeric, coercing errors to NaN and then fill NaNs with 0."""
        self.data['views'] = pd.to_numeric(self.data['views'], errors='coerce').fillna(0).astype(int)
        return self

    def convert_date_columns(self):
        """Convert 'date' and 'post_hour' columns to datetime and extract hour."""
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.data['post_hour'] = pd.to_datetime(self.data['post_hour'], format='%H:%M:%S', errors='coerce')
        self.data['hour'] = self.data['post_hour'].dt.hour
        return self

    def get_cleaned_data(self):
        """Return the cleaned data."""
        return self.data

    def clean_data(self):
        """Perform all cleaning steps."""
        self.fill_missing_values()
        self.convert_to_numeric()
        self.convert_date_columns()
        return self.get_cleaned_data()
