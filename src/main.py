""" Main script using py3.8 """
# -*- coding: utf-8 -*-
import re
import os
import abc
import time
import queue
import datetime
import logging.config
import multiprocessing

import urllib.request as request

from urllib.error import URLError

from collections.abc import Iterable

from concurrent.futures import ThreadPoolExecutor

from multiprocessing import JoinableQueue as PQueue  # Process Queue

import pymongo

from yaspin import yaspin
from yaspin.spinners import Spinners

from bs4 import BeautifulSoup

from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.remote.remote_connection import Command
from selenium.common.exceptions import InvalidSessionIdException

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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

client = pymongo.MongoClient(os.getenv('MG_CRED'), port=27017)

home_url = 'https://www.linio.com.co/'

unique_ = {}  # Using like system-cached


class Singlenton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Firefox(metaclass=Singlenton):

    @staticmethod
    def create_driver_session(session_id, executor_url):
        # Source: https://tarunlalwani.com/post/reusing-existing-browser-session-selenium/
        from selenium.webdriver.remote.webdriver import WebDriver as RemoteWebDriver

        # Save the original function, so we can revert our patch
        org_command_execute = RemoteWebDriver.execute

        def new_command_execute(self, command, params=None):
            if command == "newSession":
                # Mock the response
                return {'success': 0, 'value': None, 'sessionId': session_id}
            else:
                return org_command_execute(self, command, params)

        # Patch the function before creating the driver object
        RemoteWebDriver.execute = new_command_execute

        options = Options()
        options.headless = True

        new_driver = webdriver.Remote(
            command_executor=executor_url,
            options=options,
            desired_capabilities={},
        )
        new_driver.session_id = session_id

        # Replace the patched function with original function
        RemoteWebDriver.execute = org_command_execute

        return new_driver

    def is_session_active(self):
        try:
            response = self._driver.execute(Command.STATUS)['value']
            return bool(response.get('message', False))
        except (KeyError, TypeError):
            return False

    def re_create_session(self):
        try:
            response = self._driver.execute(Command.NEW_SESSION)['value']
            return response['sessionId']
        except (KeyError, TypeError):
            return None

    def __init__(self, headless=True):
        self.headless = headless
        options = Options()
        options.headless = self.headless
        self._driver = webdriver.Firefox(
            options=options,
            executable_path=os.path.join(BASE_DIR, 'data', 'geckodriver'),
        )
        self.session_id = self._driver.session_id
        self.executor_url = self._driver.command_executor._url
        logger.info(f'Initializing Browser session: {self.session_id}, executor: {self.executor_url}')

    @property
    def driver(self):
        if not self.is_session_active():
            self.session_id = self.re_create_session()
        return self.create_driver_session(
            session_id=self.session_id,
            executor_url=self.executor_url
        )

    def __enter__(self):
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(exc_type, exc_val, exc_tb)

    def __del__(self):
        try:
            self._driver.quit()
        except:
            pass


def get_database():
    db = client.linio_data
    return db.products


def download_b64_image(url, **kwargs):
    silent = kwargs.get('silent', False)
    try:
        with Firefox() as driver:  # TODO Manage Race Condition
            driver.get(url)
            return driver.get_screenshot_as_base64()
    except (KeyError, WebDriverException):
        logger.exception('Error generating product img')
        if silent:
            return None
        raise


def insert_product_screenshot(product):  # TODO Testing
    prod = dict(product)
    screenshot = download_b64_image(prod['full_url'], silent=True)  # I/0
    prod['prices'][0]['screenshot'] = screenshot
    return prod


def insert_product(data, **kwargs):
    silent = kwargs.get('silent', False)
    logger.debug(f'INSERTING: {data}')
    try:
        database = kwargs['database']
        product = dict(data['product'])  # Not test yet insert_product_screenshot(data['product'])
        sku = product['sku']
        if sku in unique_:
            return None
        unique_[sku] = 1
        if not database.find_one({'sku': sku}):
            database.insert_one(product)
        else:
            database.find_and_modify(
                query={'sku': product['sku']},
                update=[{
                    '$set': {'prices': {'$concatArrays': ["$prices", product['prices']]}}
                }]
            )
    except (KeyError, TypeError, IndexError):
        logger.exception('Error inserting db data')
        if silent:
            return None
        raise


def get_main_content(data, **kwargs):
    silent = kwargs.get('silent', True)
    try:
        url = data['url']
        logger.debug(f'Requesting: {url}')
        res = request.urlopen(url=url)
        content = res.read()
        process_queue.put({
            'url': data['url'],
            'base_url': data['base_url'],
            'html_content': content,
            'process': data['callback'],
        })
    except URLError:
        logger.exception('Error getting server data')
        if silent:
            return None
        raise


class Strategy(abc.ABC):
    """ Strategy Interface """

    @abc.abstractmethod
    def process(self, pipe, html_content, base_url, data):
        """ Method to define logic to implement """


class CategoryProcess(Strategy):

    @staticmethod
    def parse_category(base_url, categories, silent=True):
        if not isinstance(categories, Iterable):
            if silent:
                return None
            raise ValueError('categories param must be especified')
        return {
            cat.attrs['title']: f'{base_url[:-1]}{cat.attrs["href"]}' for cat in categories
        }

    @staticmethod
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

    def process(self, pipe, html_content, base_url, data):
        logger.debug('Extracting Categories')
        categories = self.parse_category(
            base_url=base_url,
            categories=self.extract_categories(
                html_content=html_content,
            ),
        )
        for category, cat_url in categories.items():
            pipe.send({
                'url': cat_url,
                'base_url': base_url,
                'route': 'request',
                'callback': 'pages'
            })
        return True


class PageProcess(Strategy):

    @staticmethod
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

    def process(self, pipe, html_content, base_url, data):
        logger.debug('Processing Pages...')
        cat_url = data['url']
        pages = [cat_url]
        pages.extend(
            self.extract_pages(
                base_url=cat_url,
                content=html_content,
            )
        )
        for cat_page in pages:
            pipe.send({
                'url': cat_page,
                'base_url': base_url,
                'route': 'request',
                'callback': 'products',
            })
        return True


class ProductProcess(Strategy):

    @staticmethod
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

    @staticmethod
    def parse_product(product_div, base_url, silent=True):
        try:
            data = {
                meta.attrs['itemprop']: meta.attrs['content'] for meta in product_div.findAll('meta')
            }
            data['full_url'] = f'{base_url[:-1]}{data["url"]}'
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

    def process(self, pipe, html_content, base_url, data):
        logger.debug('Processing Products...')
        for product in self.extract_category_products(category_html=html_content):
            if not product:
                continue
            pipe.send({
                'route': 'database',
                'product': self.parse_product(product, base_url=base_url),
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

    def process(self, pipe, html_content, base_url, data):
        return self.strategy.process(pipe, html_content, base_url, data)


def process_worker(pipe):
    while True:
        try:
            data = process_queue.get(timeout=30)  # Maximum 1/2 minutes
        except queue.Empty:
            pipe.send('finish')
            return
        try:
            html_content = data.pop('html_content')
            base_url = data.pop('base_url')
            logger.debug(f'Processing Tasks: {data}')
            manager = Context(data['process'])
            manager.process(
                pipe=pipe,
                html_content=html_content,
                base_url=base_url,
                data=data,
            )
        finally:
            process_queue.task_done()


def process_worker_msg(msg, route, pooling):
    if request_ == 'finish':
        return False
    elif not isinstance(msg, dict):
        logger.warning(f'Process message is not a dict o.0?, {request_}')
        return True
    route_ = request_.pop('route', None)
    fnc = route.get(route_, None)
    if not fnc:
        logger.critical(f'Route: {route_} is not defined o.0?')
        return True
    pooling.submit(fnc, **{'data': request_, 'database': collection})
    return True


if __name__ == '__main__':
    logger.info(f'Start program {datetime.datetime.now()}')
    # Not test yet, Firefox()
    collection = get_database()
    collection.create_index('sku', **{'unique': True})
    start = time.perf_counter()
    parent_conn, child_conn = multiprocessing.Pipe()
    multiprocessing.Process(target=process_worker, args=(child_conn,), daemon=True).start()
    router = {
        'request': get_main_content,
        'database': insert_product,
    }
    kwg = {
        'url': home_url,
        'base_url': home_url,
        'callback': 'category',
    }
    with yaspin(spinner=Spinners.monkey, text='Scrapping product information', color='green') as sp:
        with ThreadPoolExecutor() as thread_pooling:
            thread_pooling.submit(router['request'], **{'data': kwg})
            while True:
                request_ = parent_conn.recv()
                if not process_worker_msg(msg=request_, pooling=thread_pooling, route=router):
                    parent_conn.close()
                    break
            sp.text = 'All info was requested, inserting on database...'
        end = time.perf_counter()
        sp.text = f'Process Finished in {end - start}s :D'
        sp.ok('✅')
