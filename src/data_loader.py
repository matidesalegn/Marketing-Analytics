# src/data_loader.py
import pandas as pd

class DataLoader:
    def __init__(self, file_path, separator='|'):
        self.file_path = file_path
        self.separator = separator
        self.data = None

    def load_data(self):
        self.data = pd.read_csv(self.file_path, sep=self.separator, low_memory=False)
        return self.data

    def display_head(self, n=5):
        if self.data is not None:
            return self.data.head(n)
        else:
            print("Data is not loaded. Please load the data first.")

    def basic_info(self):
        if self.data is not None:
            return self.data.info()
        else:
            print("Data is not loaded. Please load the data first.")