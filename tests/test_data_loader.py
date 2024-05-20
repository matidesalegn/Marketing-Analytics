# tests/test_data_loader.py
import unittest
from src.data_loader import DataLoader

class TestDataLoader(unittest.TestCase):
    def test_load_data(self):
        loader = DataLoader("data/Apollo android review data.xlsx")
        data = loader.load_data()
        self.assertIsNotNone(data)
        self.assertTrue(data.shape[0] > 0)

if __name__ == "__main__":
    unittest.main()