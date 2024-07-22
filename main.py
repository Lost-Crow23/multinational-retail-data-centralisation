from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import os 
from dotenv import load_dotenv

if __name__ == "__main__":
    db_connector = DatabaseConnector()
    data_extract = DataExtractor()
    data_clean = DataCleaning()

    load_dotenv('.env')

    engine = db_connector.init_db_engine()

    # user_data
    user_data = data_extract.read_rds_table(engine, "legacy_users")
    clean_user_data = data_clean.clean_user_data(user_data)

    # card details
    card_details = data_extract.retrieve_pdf_data()
    clean_card_details = data_clean.clean_card_data(card_details)

    # store details 
    endpoint = os.getenv('number_of_stores_endpoint')
    store_endpoint = os.getenv('store_details_endpoint')
    api_key = os.getenv('x-api-key')

    number_of_stores = data_extract.list_number_of_stores(endpoint, api_key)
    store_details = data_extract.retrieve_stores_data(store_endpoint, api_key)
    clean_store = data_clean.clean_store_data(store_details)

    # # product details 
    product_address = os.getenv('product_address')
    product_data = data_extract.extract_from_s3(product_address)
    clean_product = data_clean.clean_products_data(product_data)  

    # orders data 
    order_data = data_extract.read_rds_table(engine, "orders_table")
    clean_order_data = data_clean.clean_orders_data(order_data)

    # # order time data 
    order_time_address = os.getenv('order_time_address')
    order_time_data = data_extract.extract_from_s3(order_time_address)
    clean_order_time_data = data_clean.clean_order_date(order_time_data)

    # Upload to Local Database
    db_connector.upload_to_db(clean_user_data, 'dim_user') 
    db_connector.upload_to_db(clean_card_details, 'dim_card_details') 
    db_connector.upload_to_db(clean_store, 'dim_store_details') 
    db_connector.upload_to_db(clean_product, 'dim_products') 
    db_connector.upload_to_db(clean_order_data, 'orders_table')  
    db_connector.upload_to_db(clean_order_time_data, 'dim_date_times')
