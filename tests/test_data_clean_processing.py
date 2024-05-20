# tests/test_data_clean_processing.py
import unittest
import pandas as pd
from src.data_clean_processing import DataCleanProcessing

class TestDataCleanProcessing(unittest.TestCase):
    def test_drop_missing_values(self):
        data = pd.DataFrame({'A': [1, 2, None], 'B': [None, 2, 3]})
        clean_process = DataCleanProcessing(data)
        cleaned_data = clean_process.drop_missing_values()
        self.assertEqual(cleaned_data.isnull().sum().sum(), 0)

    def test_remove_duplicates(self):
        data = pd.DataFrame({'A': [1, 2, 2], 'B': [1, 2, 2]})
        clean_process = DataCleanProcessing(data)
        cleaned_data = clean_process.remove_duplicates()
        self.assertEqual(cleaned_data.duplicated().sum(), 0)

if __name__ == "__main__":
    unittest.main()
