import unittest
from src.data_clean_processing import DataCleanProcessing
import pandas as pd

class TestDataCleanProcessing(unittest.TestCase):

    def setUp(self):
        # Setup a small sample dataframe for testing
        self.sample_data = pd.DataFrame({
            'A': [1, 2, None, 4],
            'B': ['a', 'b', 'c', None]
        })
        self.data_cleaner = DataCleanProcessing(self.sample_data)

    def test_clean_missing_values(self):
        cleaned_data = self.data_cleaner.clean_missing_values()
        # Check if missing values are filled
        self.assertFalse(cleaned_data.isnull().values.any())
        self.assertEqual(cleaned_data.loc[2, 'A'], self.sample_data['A'].median())
        self.assertEqual(cleaned_data.loc[3, 'B'], self.sample_data['B'].mode()[0])

    def test_verify_no_missing_values(self):
        self.data_cleaner.clean_missing_values()
        self.assertTrue(self.data_cleaner.verify_no_missing_values())

if __name__ == '__main__':
    unittest.main()
