# tests/test_data_loader.py
import unittest
from src.data_loader import DataLoader

class TestDataLoader(unittest.TestCase):
    def setUp(self):
        self.file_path = '../data/MachineLearningRating_v3.txt'
        self.data_loader = DataLoader(self.file_path)

    def test_load_data(self):
        data = self.data_loader.load_data()
        self.assertIsNotNone(data)

    def test_display_head(self):
        self.data_loader.load_data()
        head = self.data_loader.display_head()
        self.assertEqual(len(head), 5)

    def test_basic_info(self):
        self.data_loader.load_data()
        info = self.data_loader.basic_info()
        self.assertIsNone(info)

if __name__ == '__main__':
    unittest.main()