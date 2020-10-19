""" Main script """
import re
import os
import time
import datetime
import abc
import logging.config
import multiprocessing
import urllib.request as request
from urllib.error import URLError

import queue

from concurrent.futures import ThreadPoolExecutor

from multiprocessing import JoinableQueue as PQueue  # Process Queue

from collections.abc import Iterable

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

import pymongo
from pymongo import MongoClient

from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_config = {
    'version': 1,
    'formatters': {
        'brief': {
            'fmt': '%(level): %(asctime) - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'brief',
            'level': 'DEBUG',
            'stream': 'ext://sys.stdout',
        },
        'file_handler': {
            'class': 'logging.FileHandler',
            'formatter': 'brief',
            'filename': 'logs.log'
        }
    },
    'loggers': {
        'main': {
            'level': 'INFO',
            'handlers': ['console', 'file_handler'],
        },
    },
}

logging.config.dictConfig(log_config)

logger = logging.getLogger('main')

process_queue = PQueue()

client = MongoClient(
    ''
)
db = client.linio_data
db_products = db.products

home_url = 'https://www.linio.com.co/'


def download_b64_image(url):
    options = Options()
    options.headless = True
    driver = webdriver.Firefox(
        firefox_options=options,
        executable_path=os.path.join(BASE_DIR, 'data', 'geckodriver'),
    )
    driver.get(url)
    return driver.get_screenshot_as_base64()


def get_product_url(sku):
    search_url = f'{home_url}search?scroll=&q={sku}'
    res = request.urlopen(search_url)
    return res.url


"""LEGACY
def get_database(db_name='ldata'):
    from codernitydb3.database import Database
    db = Database(db_name)
    if not db.exists():
        db.create()
    else:
        db.open()
    return db
"""


def insert_product(data, silent=True):
    product = dict(data['product'])
    logger.debug(f'INSERTING: {product}')
    if not db_products.find_one({'sku': product['sku']}):
        db_products.insert_one(product)
    else:
        db_products.find_and_modify({
            'query': {'sku': product['sku']},
            'update': [{
                '$set': {'prices': {'$concatArrays': ["$prices", product['prices']]}}
            }]
        })


def get_main_content(data, silent=True):
    try:
        url = data['url']
        logger.debug(f'Requesting: {url}')
        res = request.urlopen(url=url)
        content = res.read()
        process_queue.put({
            'url': data['url'],
            'html_content': content,
            'process': data['callback'],
        })
    except URLError:
        if silent:
            return None
        logger.exception('Error getting server data')
        raise


def parse_category(base_url, categories, silent=True):
    if not isinstance(categories, Iterable):
        if silent:
            return None
        raise ValueError('categories param must be especified')
    return {
        cat.attrs['title']: f'{base_url[:-1]}{cat.attrs["href"]}' for cat in categories
    }


def extract_categories(html_content, silent=True):
    if not isinstance(html_content, str) and not isinstance(html_content, bytes):
        if silent:
            return None
        raise ValueError('html_content param must be str object.')
    soup = BeautifulSoup(html_content, 'html.parser')
    nav_bar = soup.findAll('nav', {'itemtype': 'http://www.schema.org/SiteNavigationElement'})
    if not nav_bar:
        if silent:
            return None
        raise ValueError('HTML content is not valid to extract categories.')
    if nav_bar.__len__() > 1:
        if silent:
            return None
        raise ValueError('Verify HTML content return unexpected results getting navbar.')
    nav_bar = nav_bar[0]
    return nav_bar.findAll('a')


def parse_product(product_div, silent=True):
    try:
        data = {
            meta.attrs['itemprop']: meta.attrs['content'] for meta in product_div.findAll('meta')
        }
        data['full_url'] = f'{home_url}{data["url"]}'
        prices = [
            {
                'price': data.pop('price', 0),
                'priceCurrency': data.pop('priceCurrency', 'UNDEFINED'),
                'registered_date': datetime.datetime.utcnow(),
            },
        ]
        data['prices'] = prices
        return data
    except (AttributeError, KeyError):
        if silent:
            return {}
        raise TypeError('product_div param type is invalid, expected iterable.')


def extract_category_products(category_html, silent=True):
    try:
        parser = BeautifulSoup(category_html, 'html.parser')
    except TypeError:
        if silent:
            return []
        raise
    product_div = parser.find('div', {'id': 'catalogue-product-container'})
    if not product_div:
        return []
    products = product_div.findAll('div', {'itemtype': 'http://schema.org/Product'})
    if not products:
        return []
    for product in products:
        yield product


def extract_pages(base_url, content, silent=True):
    try:
        content = BeautifulSoup(content, 'html.parser')
    except TypeError:
        if silent:
            return []
        raise
    pages_link = content.findAll('li', {'class': 'page-item'})
    if not pages_link:
        return []

    def get_link(li_):
        try:
            return li_.find('a').attrs['href']
        except (KeyError, AttributeError):
            return ''

    rgx = re.compile(r'\d+')
    try:
        max_page = max(
            map(
                lambda link: int(rgx.findall(get_link(link))[0]) if rgx.search(get_link(link)) else -1,
                pages_link
            )
        )
    except (IndexError, ValueError):
        if silent:
            return []
        raise
    return [f'{base_url}?page={page}' for page in range(2, max_page + 1)]


class Strategy(abc.ABC):
    """ Strategy Interface """

    @abc.abstractmethod
    def process(self, pipe, html_content, data):
        """ Method to define logic to implement """


class CategoryProcess(Strategy):

    def process(self, pipe, html_content, data):
        logger.debug('Extracting Categories')
        categories = parse_category(
            base_url=home_url,
            categories=extract_categories(
                html_content=html_content,
            ),
        )
        for category, cat_url in categories.items():
            logger.debug('Sending jobs.')
            pipe.send({
                'url': cat_url,
                'route': 'request',
                'callback': 'pages'
            })
        return True


class PageProcess(Strategy):

    def process(self, pipe, html_content, data):
        logger.debug('Processing Pages...')
        cat_url = data['url']
        pages = [cat_url]
        pages.extend(
            extract_pages(
                base_url=cat_url,
                content=html_content,
            )
        )
        for cat_page in pages:
            pipe.send({
                'url': cat_page,
                'route': 'request',
                'callback': 'products',
            })
        return True


class ProductProcess(Strategy):

    def process(self, pipe, html_content, data):
        logger.debug('Processing Products...')
        for product in extract_category_products(category_html=html_content):
            if not product:
                continue
            pipe.send({
                'route': 'database',
                'product': parse_product(product),
            })
        return True


class Context:
    strategies = {
        'category': CategoryProcess,
        'pages': PageProcess,
        'products': ProductProcess,
    }

    def __init__(self, strategy):
        if strategy not in self.strategies:
            raise ValueError(f'Strategy: {strategy} not supported yet')
        self.strategy = self.strategies[strategy]()

    def process(self, pipe, html_content, data):
        return self.strategy.process(pipe, html_content, data)


def process_worker(pipe):
    while True:
        try:
            data = process_queue.get(timeout=30)  # Maximum 1/2 minutes
        except queue.Empty:
            pipe.send('finish')
            return
        try:
            html_content = data.pop('html_content')
            logger.debug(f'Processing Tasks: {data}')
            manager = Context(data['process'])
            manager.process(
                pipe=pipe,
                html_content=html_content,
                data=data,
            )
        finally:
            process_queue.task_done()


if __name__ == '__main__':
    logger.info(f'Start program {datetime.datetime.now()}')
    db_products.create_index('sku', **{"unique": True})
    start = time.perf_counter()
    parent_conn, child_conn = multiprocessing.Pipe()
    thread_pooling = ThreadPoolExecutor(max_workers=5)
    multiprocessing.Process(target=process_worker, args=(child_conn,), daemon=True).start()
    router = {
        'request': get_main_content,
        'database': insert_product,
    }
    kwg = {
        'url': home_url,
        'callback': 'category',
    }
    thread_pooling.submit(router.get('request'), **{'data': kwg})
    while True:
        request_ = parent_conn.recv()
        if request_ == 'finish':
            break
        elif not isinstance(request_, dict):
            continue
        route = request_.pop('route', None)
        fnc = router.get(route, None)
        if not fnc:
            continue
        logger.debug(f'Sending to pool job: {fnc.__name__}')
        thread_pooling.submit(fnc, **{'data': request_})
    parent_conn.close()
    # TODO ThreadPooling finish before time.
    end = time.perf_counter()
    logger.info(f'Process Finished in {end - start}s :D')
