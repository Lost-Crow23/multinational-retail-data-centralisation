from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd 
import numpy as np 
from datetime import datetime
import re
from dotenv import load_dotenv
class DataCleaning():
    def __init__(self):
        pass
    """
    This class produces the cleaning methods for the cleaning data, date errors, handling NULL values, 
    and any incorrect data types in datasets. it is used to clean user data, card data, store data, products data, 
    orders data, and JSON data.
    
    Public Methods for User Data:
    1. clean_user_data(user_data)
        - Cleans the user data by handling date errors, dropping rows with NULL values and filtering incorrect data rows.
        - Parameters:
            - user_data(DataFrame): The user data to be cleaned
        - Returns:
            - DataFrame: The cleaned user data
    
    Public Methods for Card Data:
    2. clean_card_data(card_data)
        - Cleans the card data by handling date errors, NULL values, and formatting errors found in card numbers.
        - Parameters:
            - card_data(DataFrame): The card data to be cleaned
        - Returns:
            - DataFrame: The cleaned card data
    
    Public Methods for Store Data:
    3. clean_store_data(stores_data)
        - Cleans the store data by removing random or with missing values in the 'weight' column and converting the products weight
        - Parameters:
            - stores_data(DataFrame): The store data to be cleaned
        - Returns:
            - DataFrame: The cleaned store data

    Public Methods for Products Data:
    4. clean_products_data(products_data):
        - Cleans the products data by removing duplicates. invalid dates, correcting continent names and converting the date column.
        - Parameters:
            - products_data(DataFrame): The products data to be cleaned
        - Returns:
            - DataFrame: The cleaned products data

    Public Methods for Orders Data:
    5. clean_orders_data(orders_table):
        - Cleans the orders data by dropping specific columns.
        - Parameters:
            - orders_data(DataFrame): The orders data to be cleaned
        - Returns:
            - DataFrame: The cleaned orders data 

    Public Methods for Orders Data:
    6. clean_order_date(date_details):
        - Cleans the orders date data by converting date columns, filtering time periods and dropping rows with invalid dates
        - Parameters:
            - orders_date(DataFrame): The orders date to be cleaned
        - Returns:
            - DataFrame: The cleaned orders date data
    """
# 1. Public method for user data
    def clean_user_data(self, user_data):
        print("running clean user data")
        clean_user_data = user_data.copy()
        clean_user_data = self.remove_duplicate_rows(clean_user_data)
        clean_user_data = self.replace_index_column(clean_user_data)
        clean_user_data = self.drop_rows_with_null_values(clean_user_data)
        clean_user_data = self.first_name_last_name_values(clean_user_data)
        clean_user_data = self.format_date_time_values(clean_user_data)
        clean_user_data = self.correct_company_values(clean_user_data)
        clean_user_data = self.valid_email(clean_user_data)
        clean_user_data = self.valid_address(clean_user_data)
        clean_user_data = self.valid_phone_number(clean_user_data)
        print("cleaning user data done\n")
        return clean_user_data
    
    # Private methods for user data
    def _remove_duplicate_rows(self, df):
        return df.drop_duplicates(subset= ['user_uuid'])
    
    def _replace_index_column(self, df):
        df.reset_index(inplace = True)
        df = df.drop(['level_0'], axis=1)
        df.drop(df.columns[0], axis=1, inplace=True)
        df['index'] = range(1, len(df) + 1)
        return df
    
    def _drop_rows_with_null_values(self, df):
        df = df.replace("NULL", np.NaN)
        df = df.dropna(subset = ['user_uuid'], how='any', axis=0)
        df = df.drop_duplicates(subset = ['phone_number', 'email_address'])# fix this
        return df
    
    def _first_name_last_name_values(self, df):
        df['valid_first_name'] = df['first_name'].str.match(r'^[A-Za-z ]+$')
        df['valid_last_name'] = df['last_name'].str.match(r'^[A-Za-z ]+$')
        return df
    
    def _format_date_time_values(self, df):
        df['join_date'] = df['join_date'].apply(pd.to_datetime, errors = 'coerce')
        df.dropna(subset = ['join_date'], inplace = True) # comment
        return df

    def _correct_company_values(self, df):
        df['company'] = df['company'].str.title()
        return df
    
    def _valid_email(self, df):
        pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$'
        df = df[df['email_address'].str.match(pattern, na= False)]
        return df
    
    def _valid_address(self, df):
        df['address'] = df['address'].str.title()
        df['address'] = df['address'].str.replace(".", "", regex = False)
        df['address'] = df['address'].str.replace("/", "", regex = False)
        df['address'] = df['address'].str.replace("...", "", regex = False)
        return df

    def _valid_phone_number(self, df):
        df['phone_number'] = df.apply(lambda row: re.sub(fr'^\+44|^\+49|^{row["country_code"]}','', row['phone_number']), axis = 1)
        df['phone_number'] = df['phone_number'].apply(lambda phone: re.sub(r'\D', '', phone))
        df['phone_number'] = df['phone_number'].apply(lambda phone: '0' + phone if not phone.startswith('0') else phone)
        return df
    
# 2. Public methods for card data
    def clean_card_data(self, card_data):
        print("cleaning card data")
        clean_card_data = card_data.copy()
        clean_card_data = self.drop_null_values(clean_card_data)
        clean_card_data = self.cleaning_card_number(clean_card_data)
        clean_card_data = self.parse_date_format(clean_card_data)
        clean_card_data = self.remove_long_expiry_dates(clean_card_data)
        clean_card_data = self.remove_short_expiry_dates(clean_card_data)
        clean_card_data = self.remove_non_numeric_symbols(clean_card_data, 'card_number')
        print("cleaning card data done\n")
        clean_card_data.reset_index(drop=True, inplace=True)
        
        return clean_card_data
    
    # Private methods for card data
    def _drop_null_values(self, df):
        df = df.replace("NULL", np.NaN)
        df = df.drop_duplicates(subset = ['card_number'])
        df.dropna()
        return df
    
    def _cleaning_card_number(self, df):
        df['card_number'] = df['card_number'].apply(str)
        df['card_number'] = df['card_number'].str.replace(r'\?', '', regex = True)
        df = df[~df['card_number'].str.contains(r'[a-zA-Z]')]
        return df
    
    def _parse_date_format(self, df):
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format = '%Y-%m-%d', errors = 'coerce').dt.date
        return df

    def _remove_long_expiry_dates(self, df):
        df = df[df['expiry_date'].str.len() <= 5]
        return df
    
    def _remove_short_expiry_dates(self, df):
        df = df[df['card_number'].str.len() >= 8]
        return df 
    
    def _remove_non_numeric_symbols(self, df, column_name):
        df[column_name] = df[column_name].str.replace(r'[^0-9]', '', regex = True)
        return df 
    
# 3. Public methods for store data
    def clean_store_data(self, stores_data):
        print("cleaning store data")
        cleaned_store_data = stores_data.copy()
        cleaned_store_data = self.clean_date_columns(cleaned_store_data)
        cleaned_store_data = self.drop_store_null_values(cleaned_store_data)
        cleaned_store_data = self.remove_non_numeric_staff(cleaned_store_data)
        cleaned_store_data = self.correct_continent_codes(cleaned_store_data)
        cleaned_store_data = self.cleaning_address(cleaned_store_data)
        cleaned_store_data = self.drop_invalid_store_type(cleaned_store_data)
        cleaned_store_data = self.clean_longitude(cleaned_store_data)
        cleaned_store_data.reset_index(drop = True, inplace = True)
        print("cleaning store data done\n")
        return cleaned_store_data 

    # Private methods for card data
    def _clean_date_columns(self, df):
        df['opening_date'] = pd.to_datetime(df['opening_date'], format = 'mixed', errors = 'coerce').dt.date
        return df
    
    def _remove_non_numeric_staff(self, df):
        if df is None:
            raise ValueError("the dataframe is None")
        if 'staff_numbers' not in df.columns:
            raise ValueError("'staff_numbers' column missing from the datafame")
        if df['staff_numbers'] is None:
            raise ValueError("'staff_numbers' column is None")
        df['staff_numbers'] = df['staff_numbers'].str.replace(r"(\D)", "", regex = True)
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors = 'coerce', downcast='integer')
        return df
    
    def _drop_store_null_values(self, df):
        df = df.drop(columns = ['lat'])
        df = df.drop(columns = ['index'])
        df = df.rename(columns = {"level_0": "index"})
        df = df.replace("NULL", np.NaN)
        df = df.dropna(subset = ['staff_numbers'], axis = 0)
        return df

    def _correct_continent_codes(self, df):
        df['continent'] = df['continent'].str.replace('eeEurope', 'Europe')
        df['continent'] = df['continent'].str.replace('eeAmerica', 'America')
        df = df[df['country_code'].str.len() <= 2]
        return df

    def _cleaning_address(self, df):
        df['address'] = df['address'].str.replace('\n' , ' ')
        df['address'] = df['address'].str.replace('/', '')
        return df 

    def _drop_invalid_store_type(self, df):
        df = df[df["store_type"].str.contains("Web Portal|Local|Super Store|Outlet|Mall Kiosk")]                      
        return df

    def _clean_longitude(self, df):
        df['longitude'] = df['longitude'].round(5)
        return df

    # Private methods for products weights
    def _convert_product_weights(self, weight):
        weight = weight.rstrip(". ") # Removes . and any trailing white spaces
        def convert(weight):
            weight_in_kg = weight    
            if "kg" in weight:
                weight_in_kg = float(weight.replace("kg", ""))
            elif "g" in weight:
                weight_in_kg = float(weight.replace("g", ""))
                weight_in_kg = weight_in_kg/1000
            elif "ml" in weight:
                weight_in_kg = float(weight.replace("ml", ""))
                weight_in_kg = weight_in_kg/1000
            elif "lb" in weight:
                weight_in_kg = float(weight.replace("lb", ""))
                weight_in_kg = weight_in_kg*0.453591
            elif "oz" in weight:
                weight_in_kg = float(weight.replace("oz", ""))
                weight_in_kg = weight_in_kg*0.0283495
            return weight_in_kg 
        
        if "x" in weight:
            var1, var2 = weight.split(" x ")
            var2 = convert(var2)
            weight_in_kg = int(var1) * var2
            return weight_in_kg
        else:
            weight_in_kg = convert(weight)
            return weight_in_kg

# 4. Public methods for products data
    def clean_products_data(self, products_data):
        print("running clean products data")
        cleaned_products_data = products_data.copy()
        cleaned_products_data = self.remove_null_values(cleaned_products_data)
        cleaned_products_data = self.clean_weight_column(cleaned_products_data)
        cleaned_products_data = self.convert_date_added_column(cleaned_products_data)
        cleaned_products_data = self.category_list(cleaned_products_data)
        cleaned_products_data = self.missing_and_random_prices(cleaned_products_data, column = 'product_price')
        print("cleaning products_data done\n")
        return cleaned_products_data

    #  Private methods for products data
    def _remove_null_values(self, df):
        df = df.replace("Null", np.NaN)
        df = df.dropna(subset = ['weight'])
        df.drop(df.columns[0], axis = 1, inplace = True)
        return df
        
    def _clean_weight_column(self, df):
        weight_in_kg = []
        for weight in df['weight']:
            correct_weight = self.convert_product_weights(weight)
            weight_in_kg.append(correct_weight)
        df['weight'] = weight_in_kg
        return df

    def _convert_date_added_column(self, df):
        df['date_added'] = df['date_added'].apply(pd.to_datetime, errors = 'coerce')
        df = df.dropna(subset = ['date_added'])
        return df 
    
    def _category_list(self, df):
        category_list = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 
                            'food_and_drink', 'diy']
        df = df[df['category'].isin(category_list)]
        return df
    
    def _missing_and_random_prices(self, df, column):
        pattern = r'^[a-zA-Z0-9]*$'
        mask = df[column].str.match(pattern)
        df = df[~mask]
        return df

# 5. Public methods for orders data
    def clean_orders_data(self, orders_table):
        print("running clean orders data")
        cleaned_orders_data = orders_table.copy()
        cleaned_orders_data = self.drop_columns(cleaned_orders_data)
        cleaned_orders_data = self.reset_index(cleaned_orders_data)
        cleaned_orders_data = self.clean_card_number(cleaned_orders_data)
        print("cleaning orders data done\n")
        return cleaned_orders_data
    
    # Private method for orders data
    def _drop_columns(self, df):
        df = df.drop(['level_0', 'first_name', 'last_name', '1'], axis = 1)
        return df

    def _reset_index(self, df):
        df.drop(df.columns[0], axis = 1, inplace = True)
        return df 
    
    def _clean_card_number(self, df):
        df['card_number'] = df['card_number'].apply(str)
        df['card_number'] = df['card_number'].str.replace(r'\?', '', regex = True)
        df = df.loc[df['card_number'].str.len() >= 14]
        df = df.loc[df['card_number'].str.len() <= 19]
        return df 

    def _clean_order_date(self, date_details):
        clean_order_date = date_details.copy()
        print("running clean_order_date")
        clean_order_date = self.filter_invalid_time_periods(clean_order_date)
        clean_order_date = self.convert_json_columns(clean_order_date)
        print("cleaning order date done\n")
        return clean_order_date

    def _filter_invalid_time_periods(self, df):
        valid_time_periods = ['Morning', 'Evening', 'Midday', 'Late_Hours']
        return df[df['time_period'].isin(valid_time_periods)]
    
    def _convert_json_columns(self, df):
        df['month'] = pd.to_numeric(df['month'], errors = 'coerce')
        df['year'] = pd.to_numeric(df['year'], errors = 'coerce')
        df['day'] = pd.to_numeric(df['day'], errors = 'coerce')
        df = df.dropna(subset = ['month', 'year', 'day'])
        return df

# Calls out this function 
if __name__ =="__main__":
    DataCleaning()