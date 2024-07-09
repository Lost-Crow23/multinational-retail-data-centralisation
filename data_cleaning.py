from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd 
import numpy as np 
from datetime import datetime
import re
from dotenv import load_dotenv
import os

class DataCleaning():
    def __init__(self):
        pass
        
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
        return clean_user_data

    def remove_duplicate_rows(self, df):
        return df.drop_duplicates(subset= ['user_uuid'])
    
    def replace_index_column(self, df):
        df.reset_index(inplace = True)
        df.drop(df.columns[0], axis=1, inplace=True)
        df['index'] = range(1, len(df) + 1)
        return df
    
    def drop_rows_with_null_values(self, df):
        df = df.replace("NULL", np.NaN)
        df = df.dropna(subset = ['user_uuid'], how='any', axis=0)
        df = df.drop_duplicates(subset = ['phone_number', 'email_address']) # fix this
        return df
    
    def first_name_last_name_values(self, df):
        df['first_name'] = df['first_name'].str.match(r'^[A-Za-z ]+$')
        df['last_name'] = df['last_name'].str.match(r'^[A-Za-z ]+$')
        return df
    
    def format_date_time_values(self, df):
        # df['date_of_birth'] = df['date_of_birth'].str.match
        df['join_date'] = df['join_date'].apply(pd.to_datetime, errors = 'coerce')
        df.dropna(subset = ['join_date'], inplace = True) # comment
        return df

    def correct_company_values(self, df):
        df['company'] = df['company'].str.title()
        return df
    
    def valid_email(self, df):
        pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$'
        df = df[df['email_address'].str.match(pattern, na= False)]
        return df
    
    def valid_address(self, df):
        df['address'] = df['address'].str.title()
        df['address'] = df['address'].str.replace(".", "", regex = False)
        df['address'] = df['address'].str.replace("/", "", regex = False)
        df['address'] = df['address'].str.replace("...", "", regex = False)
        return df

    def valid_phone_number(self, df):
        df['phone_number'] = df.apply(lambda row: re.sub(fr'^\+44|^\+49|^{row["country_code"]}','', row['phone_number']), axis = 1)
        df['phone_number'] = df['phone_number'].apply(lambda phone: re.sub(r'\D', '', phone))
        df['phone_number'] = df['phone_number'].apply(lambda phone: '0' + phone if not phone.startswith('0') else phone)
        return df
    
    print("cleaning user data done\n")

    def clean_card_data(self, card_data):
        print("cleaning card data")
        clean_card_data = card_data.copy()
        clean_card_data = self.drop_null_values(clean_card_data)
        clean_card_data = self.cleaning_card_number(clean_card_data)
        clean_card_data = self.parse_date_format(clean_card_data)
        clean_card_data = self.remove_long_expiry_dates(clean_card_data)
        clean_card_data = self.remove_short_expiry_dates(clean_card_data)
        clean_card_data = self.remove_non_numeric_symbols(clean_card_data, 'card_number')
        clean_card_data.reset_index(drop=True, inplace=True)
        return clean_card_data
    
    def drop_null_values(self, df):
        df = df.replace("NULL", np.Nan)
        df = df.drop_duplicates(subset = ['card_number'])
        df.dropna()
        return df
    
    def cleaning_card_number(self, df):
        df['card_number'] = df['card_number'].apply(str)
        df['card_number'] = df['card_number'].str.replace(r'\?', '', regex = True)
        df = df.drop[df['card_number'].str.contains(r'[a-zA-Z]')]
        return df
    
    def parse_date_format(self, df):
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format = '%Y-%m-%d', errors = 'coerce').dt.date
        return df

    def remove_long_expiry_dates(self, df):
        df = df[df['expiry_date'].str.len() <= 5]
        return df
    
    def remove_short_expiry_dates(self, df):
        df = df[[df['card_number'].str.len() >= 8]]
        return df 
    
    def remove_non_numeric_symbols(self, df, column_name):
        df[column_name] = df[column_name].str.replace(r'[^0-9]', '', regex = True)
        return df 
    
    print("cleaning card data done\n")

    def clean_store_data(self, stores_data):
        print("cleaning store data")
        cleaned_store_data = stores_data.copy()
        cleaned_store_data = self.clean_date_columns(cleaned_store_data)
        cleaned_store_data = self.drop_store_null_values(cleaned_store_data)
        cleaned_store_data = self.remove_invalid_dates(cleaned_store_data)
        cleaned_store_data = self.remove_non_numeric_staff(cleaned_store_data)
        cleaned_store_data = self.correct_continent_codes(cleaned_store_data)
        cleaned_store_data = self.cleaning_address(cleaned_store_data)
        cleaned_store_data = self.drop_invalid_store_type(cleaned_store_data)
        cleaned_store_data.reset_index(drop = True, inplace = True)
        return cleaned_store_data 
    
    def drop_store_null_values(self, df):
        df = df.drop(columns = ['lat'])
        df = df.replace("NULL", np.NaN)
        df = df.dropna(subset = ['staff_numbers'], how=any, axis = 0)
        return df

    def clean_date_columns(self, df):
        df['opening_date'] = pd.to_datetime(df['opening_date'], format = 'mixed', errors = 'coerce').dt.date
        return df
    
    def remove_invalid_dates(self, df):
        pass

    def remove_non_numeric_staff(self, df):
        df['staff_number'] = df['staff_number'].str.replace(r"(\D)", "", regex = True)
        df['staff_number'] = pd.to_numeric(df['staff_number'], errors = 'coerce', downcast='integer')
        return df

    def correct_continent_codes(self, df):
        df['continent'] = df['continent'].str.replace('eeEurope', 'Europe')
        df['continent'] = df['continent'].str.replace('eeAmerica', 'America')
        df = df[df['country_code'].str.len() <= 2]
        return df

    def cleaning_address(self, df):
        df['address'] = df['address'].str.replace('\n' , ' ')
        df['address'] = df['address'].str.replace('/', '')
        return df 

    def drop_invalid_store_type(self, df):
        df = df[df["store_type"].str.contains("Web Portal|Local|Super Store|Outlet|Mall Kiosk")]                      
        return df
    
    print("cleaning store data done\n")

    def clean_products_data(self, products_df):
        print("running clean products data")
        cleaned_products_data = self.products_df.copy()
        cleaned_products_data = self.convert_product_weights(cleaned_products_data)
        cleaned_products_data = self.remove_null_values(cleaned_products_data)
        cleaned_products_data = self.clean_weight_column(cleaned_products_data)
        cleaned_products_data = self.convert_date_added_column(cleaned_products_data)
        cleaned_products_data = self.category_list(cleaned_products_data)
        cleaned_products_data = self.missing_and_random_prices(cleaned_products_data)
        cleaned_products_data = self.reset_index(drop = True, inplace= True)
        return cleaned_products_data

    def convert_product_weights(self, weight):
        weight = str(weight).rstrip(". ")
        def convert(weight):
            if "kg" in weight :
                weight_in_kg = float(weight.replace("kg", ""))
            elif "g" in weight:
                weight_in_kg = float(weight.replace("g", ""))
                weight_in_kg = weight_in_kg/1000
            elif "ml" in weight:
                weight_in_kg = float(weight.replace("ml", ""))
                weight_in_kg = weight_in_kg/1000
            elif "lb" in weight:
                weight_in_kg = float(weight.replace("lb", ""))
                weight_in_kg = weight_in_kg * 0.4543592
            elif "oz" in weight:
                weight_in_kg = float(weight.replace("oz", ""))
                weight_in_kg = weight_in_kg * 0.0283495
            return weight_in_kg
        
        if "x" in weight:
            var1, var2 = weight.split(" x ")
            var2 = convert(var2)
            weight_in_kg = int(var1) * var2
            return weight_in_kg
        else:
            weight_in_kg = convert(weight)
            return weight_in_kg
        
    def remove_null_values(self, df):
        df = df.replace("Null", np.NaN)
        df.drop(df.columns[0], axis = 1, inplace = True)
        return df

    def clean_weight_column(self, df):
        weight_in_kg = []
        for weight in df['weight']:
            correct_weight = self.convert_product_weights(weight)
            weight_in_kg.append(correct_weight)
        df['weight'] = weight_in_kg
        return df

    def convert_date_added_column(self, df):
        df['date_added'] = df['date_added'].apply(pd.to_datetime, errors = 'coerce')
        df = df.dropna(subset = ['date_added'])
        return df 
    
    def category_list(self, df):
        category_list = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 
                            'food_and_drink', 'diy']
        df = df[df['category'].isin(category_list)]
        return df
    
    def missing_and_random_prices(self, df, column):
        pattern = r'^[a-zA-Z0-9]*$'
        mask = df[column].str.match(pattern)
        df = df[~mask]
        return df
    
    print("cleaning products_data done\n")

    def clean_orders_data(self, orders_table):
        print("running clean orders data")
        cleaned_orders_data = orders_table.copy()
        cleaned_orders_data = self.drop_columns(cleaned_orders_data)
        cleaned_orders_data = self.none_values(cleaned_orders_data)
        cleaned_orders_data = self.clean_card_number(cleaned_orders_data)
        return cleaned_orders_data
    
    def drop_columns(self, df):
        df = df.drop(labels = ['level_0', 'first_name', 'last_name', '1'], axis = 1)
        return df
    
    def none_values(self, df):
        df = df.dropna
        return df
    
    def clean_card_number(self, df):
        df['card_number'] = df['card_number'].str.replace('?', '')
        df = df.loc[df['card_number'].str.len() >= 14]
        df = df.loc[df['card_number'].str.len() <= 19]
        return df 
    
    print("cleaning orders data done\n")

    def clean_order_date(self, date_details):
        clean_order_date = date_details.copy()
        print("running clean_order_date")
        clean_order_date = self.filter_invalid_time_periods(clean_order_date)
        clean_order_date = self.filter_invalid_dates(clean_order_date)
        clean_order_date = self.convert_json_columns(clean_order_date)
        return clean_order_date

    def filter_invalid_time_periods(self, df):
        valid_time_periods = ['Morning', 'Evening', 'Midday', 'Late_Hours']
        return df[df['time_period'].isin(valid_time_periods)]
    
    def filter_invalid_dates(self, df):
        pass

    def convert_json_columns(self, df):
        df['month'] = pd.to_numeric(df['month'], errors = 'coerce')
        df['year'] = pd.to_numeric(df['year'], errors = 'coerce')
        df['day'] = pd.to_numeric(df['day'], errors = 'coerce')
        df = df.dropna(subset = ['month', 'year', 'day'])
        return df
    
    print("cleaning order date done\n")

if __name__ == "__main__":
    db_connector = DatabaseConnector()
    data_extract = DataExtractor()
    data_clean = DataCleaning()

    load_dotenv('.env')

    engine = db_connector.init_db_engine()

    user_data = data_extract.read_rds_table(engine, "legacy_users")
    clean_user_data = data_clean.clean_user_data(user_data)

    card_details = data_extract.retrieve_pdf_data()
    clean_card_details = data_clean.clean_card_data(card_details)

    # store details 
    num_of_stores_endpoint = os.getenv('number_of_stores_endpoint')
    store_details_endpoint = os.getenv('store_details_endpoint')
    api_key = {os.getenv('x_api_key') : os.getenv('api_key')}

    number_of_stores = data_extract.list_number_of_stores(num_of_stores_endpoint, api_key)
    store_details = data_extract.retrieve_stores_data(store_details_endpoint, number_of_stores, api_key)
    clean_store = data_clean.clean_store_data(store_details)

    # product details 
    product_address = os.getenv('product_address')
    product_data = data_extract.extract_from_s3(product_address)
    clean_product = data_clean.clean_products_data(product_data)  

    # orders data 
    order_data = data_extract.read_rds_table(engine, "orders_table")
    clean_order_data = data_clean.clean_orders_data(order_data)

    # order time data 
    order_time_address = os.getenv('order_time_address')
    order_time_data = data_extract.extract_from_s3(order_time_address)
    clean_order_time_data = data_clean.clean_orders_data(order_time_data)

    # Upload to Local Database
    db_connector.upload_to_db(clean_user_data, 'dim_user') 
    db_connector.upload_to_db(clean_card_details, 'dim_card_details') 
    db_connector.upload_to_db(clean_store, 'dim_store_details') 
    db_connector.upload_to_db(clean_product, 'dim_products') 
    db_connector.upload_to_db(clean_order_data, 'orders_table')  
    db_connector.upload_to_db(clean_order_time_data, 'dim_date_times')
