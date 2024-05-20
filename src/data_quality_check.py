import pandas as pd

class DataQualityCheck:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
    
    def load_data(self):
        self.data = pd.read_csv(self.file_path, sep='|', low_memory=False)
        return self.data
    
    def basic_info(self):
        if self.data is not None:
            print(self.data.info())
            print(self.data.describe())
        else:
            print("Data not loaded. Call the `load_data` method first.")
    
    def check_missing_values(self):
        if self.data is not None:
            missing_values = self.data.isnull().sum()
            print("Missing values in each column:")
            print(missing_values)
        else:
            print("Data not loaded. Call the `load_data` method first.")