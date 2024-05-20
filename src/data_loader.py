# src/data_loader.py
import pandas as pd

class DataLoader:
    def __init__(self, file_path):
        self.file_path = file_path

    def load_data(self):
        try:
            data = pd.read_excel(self.file_path)
            return data
        except Exception as e:
            print(f"Error loading data from {self.file_path}: {e}")
            return None