import unittest
import pandas as pd
from src.data_cleaning import DataCleaning

class TestDataCleaning(unittest.TestCase):
    def setUp(self):
        # Sample data for testing
        self.data = pd.DataFrame({
            'keyword': ['keyword1', None, 'keyword3'],
            'views': ['100', '200', None],
            'date': ['2024-05-30', '2024-05-31', '2024-06-01'],
            'post_hour': ['10:30:00', '11:45:00', '12:15:00']
        })

    def test_fill_missing_values(self):
        # Test filling missing values in the 'keyword' column
        cleaned_data = DataCleaning(self.data).fill_missing_values().get_cleaned_data()
        self.assertEqual(cleaned_data['keyword'].tolist(), ['keyword1', 'unknown', 'keyword3'])

    def test_convert_to_numeric(self):
        # Test converting 'views' column to numeric
        cleaned_data = DataCleaning(self.data).convert_to_numeric().get_cleaned_data()
        self.assertEqual(cleaned_data['views'].dtype, 'int64')

    def test_convert_date_columns(self):
        # Test converting date columns to datetime and extracting hour
        cleaned_data = DataCleaning(self.data).convert_date_columns().get_cleaned_data()
        self.assertEqual(cleaned_data['date'].dtype, 'datetime64[ns]')
        self.assertEqual(cleaned_data['post_hour'].dtype, 'datetime64[ns]')
        self.assertEqual(cleaned_data['hour'].tolist(), [10, 11, 12])

    def test_check_missing_values(self):
        # Test checking missing values
        missing_values = DataCleaning(self.data).check_missing_values()
        self.assertEqual(missing_values['keyword'], 1)
        self.assertEqual(missing_values['views'], 1)

    def test_check_duplicates(self):
        # Test checking duplicates
        duplicates = DataCleaning(self.data).check_duplicates()
        self.assertEqual(duplicates, 0)  # No duplicates in the sample data

if __name__ == '__main__':
    unittest.main()