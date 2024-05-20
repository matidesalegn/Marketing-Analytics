import unittest
from src.data_quality_check import DataQualityCheck
import pandas as pd

class TestDataQualityCheck(unittest.TestCase):

    def setUp(self):
        # Setup a small sample dataframe for testing
        self.sample_data = pd.DataFrame({
            'A': [1, 2, None, 4],
            'B': ['a', 'b', 'c', None]
        })
        self.data_quality = DataQualityCheck('fake_path')

    def test_load_data(self):
        # Mock the load_data method
        self.data_quality.data = self.sample_data
        self.assertIsNotNone(self.data_quality.data)

    def test_basic_info(self):
        self.data_quality.data = self.sample_data
        # Just call the method to ensure it runs without error
        self.data_quality.basic_info()

if __name__ == '__main__':
    unittest.main()
