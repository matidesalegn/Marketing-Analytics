# Define the DataCleaning class
class DataCleaning:
    def __init__(self, data):
        self.data = data

    def fill_missing_values(self):
        self.data.fillna('unknown', inplace=True)
        return self

    def convert_to_numeric(self):
        self.data = self.data.apply(pd.to_numeric, errors='ignore')
        return self

    def convert_date_columns(self):
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.data['post_hour'] = pd.to_datetime(self.data['post_hour'], format='%H:%M:%S', errors='coerce')
        self.data['hour'] = self.data['post_hour'].dt.hour
        return self

    def get_cleaned_data(self):
        return self.data

    def clean_data(self):
        self.fill_missing_values()
        self.convert_to_numeric()
        self.convert_date_columns()
        return self.get_cleaned_data()