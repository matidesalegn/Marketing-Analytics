# tests/test_data_quality_check.py
import unittest
import pandas as pd
from src.data_quality_check import DataQualityCheck

class TestDataQualityCheck(unittest.TestCase):
    def test_check_missing_values(self):
        data = pd.DataFrame({'A': [1, 2, None], 'B': [None, 2, 3]})
        quality_check = DataQualityCheck(data)
        missing_values = quality_check.check_missing_values()
        self.assertEqual(missing_values['A'], 1)
        self.assertEqual(missing_values['B'], 1)

    def test_check_duplicates(self):
        data = pd.DataFrame({'A': [1, 2, 2], 'B': [1, 2, 2]})
        quality_check = DataQualityCheck(data)
        duplicates = quality_check.check_duplicates()
        self.assertEqual(duplicates, 1)

if __name__ == "__main__":
    unittest.main()