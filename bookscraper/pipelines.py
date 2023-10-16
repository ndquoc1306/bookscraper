# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2
import logging


class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        ## Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()

        ## Category & Product Type --> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        ## Price --> convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key] = float(value)

        ## Availability --> extract number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        ## Reviews --> convert string to number
        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)

        ## Stars --> convert text to number
        stars_string = adapter.get('stars')
        split_stars_array = stars_string.split(' ')
        stars_text_value = split_stars_array[1].lower()
        if stars_text_value == "zero":
            adapter['stars'] = 0
        elif stars_text_value == "one":
            adapter['stars'] = 1
        elif stars_text_value == "two":
            adapter['stars'] = 2
        elif stars_text_value == "three":
            adapter['stars'] = 3
        elif stars_text_value == "four":
            adapter['stars'] = 4
        elif stars_text_value == "five":
            adapter['stars'] = 5

        return item


class SaveToMyPostgres:

    def __init__(self, dwh_host, dwh_port, dwh_username, dwh_password, dwh_database):
        self.cur = None
        self.dwh_host = dwh_host
        self.dwh_port = dwh_port
        self.dwh_username = dwh_username
        self.dwh_password = dwh_password
        self.dwh_database = dwh_database

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            dwh_host='localhost',
            dwh_port='5432',
            dwh_username='postgres',
            dwh_password='130602',
            dwh_database='book_scraping'
        )

    def open_spider(self, spider):
        self.connection = psycopg2.connect(database=self.dwh_database,
                                           user=self.dwh_username,
                                           password=self.dwh_password,
                                           host=self.dwh_host,
                                           port=self.dwh_port)
        self.cur = self.connection.cursor()
        self.truncate_stg_table('book_scraping')

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        data = ItemAdapter(item).asdict()
        cols = data.keys()
        vals = [data[x] for x in cols]
        vals_str_list = ["%s"] * len(vals)
        vals_str = ", ".join(vals_str_list)
        cols = ', '.join(cols)
        insert_query = f"INSERT INTO book_products ({cols}) VALUES ({vals_str})"
        # try:
        self.cur.execute(insert_query, vals)
        self.connection.commit()
        # except Exception as e:
        #     logging.error(e)
        #     self.connection.rollback()
        logging.info(f'insert into stg dwh book_products row {cols}')
        return item

    def truncate_stg_table(self, table):
        # self.cur.execute(f"""delete from {table}""")
        self.connection.commit()
