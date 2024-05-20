# src/data_quality_check.py
import pandas as pd

class DataQualityCheck:
    def __init__(self, data):
        self.data = data

    def check_missing_values(self):
        missing_values = self.data.isnull().sum()
        return missing_values

    def check_duplicates(self):
        duplicates = self.data.duplicated().sum()
        return duplicates

    def summary(self):
        return {
            "missing_values": self.check_missing_values(),
            "duplicates": self.check_duplicates(),
            "data_info": self.data.info()
        }