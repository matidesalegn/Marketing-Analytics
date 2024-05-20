import pandas as pd

class DataCleanProcessing:
    def __init__(self, data):
        self.data = data
    
    def clean_missing_values(self):
        # Drop columns with more than 50% missing values
        threshold = len(self.data) * 0.5
        data_cleaned = self.data.dropna(thresh=threshold, axis=1).copy()  # Create a copy here to avoid SettingWithCopyWarning
        
        # Fill remaining missing values with the median for numerical columns or mode for categorical columns
        for column in data_cleaned.columns:
            if data_cleaned[column].dtype == 'object':
                # Fill missing values with mode for categorical columns
                mode = data_cleaned[column].mode()[0]
                data_cleaned.loc[:, column] = data_cleaned.loc[:, column].fillna(mode)
            else:
                # Fill missing values with median for numerical columns
                median = data_cleaned[column].median()
                data_cleaned.loc[:, column] = data_cleaned.loc[:, column].fillna(median)
        
        self.data = data_cleaned
        return data_cleaned
    
    def verify_no_missing_values(self):
        return self.data.isnull().sum().sum() == 0
