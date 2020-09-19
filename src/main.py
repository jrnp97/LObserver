""" Main script """
import re
import time
import threading
import logging.config
import multiprocessing
import urllib.request as request
from urllib.error import URLError

import queue
from multiprocessing import JoinableQueue as PQueue  # Process Queue
from queue import Queue  # Thread Queue

from collections.abc import Iterable

from bs4 import BeautifulSoup

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

request_queue = Queue()
process_queue = PQueue()

home_url = 'https://www.linio.com.co/'


def get_database(db_name='ldata'):
    from codernitydb3.database import Database
    db = Database(db_name)
    if not db.exists():
        db.create()
    else:
        db.open()
    return db


def get_main_content(silent=True):
    while True:
        data = request_queue.get()  # BLOCKING
        try:
            url = data['url']
            logger.info(f'Requesting: {url}')
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
        finally:
            request_queue.task_done()


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
        return {
            meta.attrs['itemprop']: meta.attrs['content'] for meta in product_div.findAll('meta')
        }
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


def process_worker(pipe):
    db = get_database('parallel_process')
    while True:
        try:
            data = process_queue.get(timeout=120)  # Maximum 2 minutes
        except queue.Empty:
            logger.info('Ending PWorker.')
            pipe.close()
            return
        try:
            html_content = data.pop('html_content')
            logger.debug(f'Processing Tasks: {data}')
            # TODO Improve with
            if data['process'] == 'category':
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
                        'callback': 'pages'
                    })
            elif data['process'] == 'pages':
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
                        'callback': 'products',
                    })
            elif data['process'] == 'products':
                for product in extract_category_products(category_html=html_content):
                    if not product:
                        continue
                    db.insert(parse_product(product))
        finally:
            process_queue.task_done()


if __name__ == '__main__':
    logger.info('Start program')
    start = time.perf_counter()
    parent_conn, child_conn = multiprocessing.Pipe()
    threading.Thread(target=get_main_content, daemon=True).start()
    threading.Thread(target=get_main_content, daemon=True).start()
    multiprocessing.Process(target=process_worker, args=(child_conn,), daemon=True).start()
    request_queue.put({
        'url': home_url,
        'callback': 'category',
    })
    while True:
        try:
            request_ = parent_conn.recv()
            request_queue.put(request_)
        except OSError:
            logger.info('Process finished')
            break
    end = time.perf_counter()
    logger.info(f'Process Finished in {end - start}s')
