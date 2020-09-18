""" Main script """
import re
import sys
import time
import urllib.request as request
from urllib.error import URLError

from collections.abc import Iterable

from bs4 import BeautifulSoup

home_url = 'https://www.linio.com.co/'

log_config = {
    'format': {},
    'logger': {},
    'handler': {}
}


def get_database(db_name='ldata'):
    from codernitydb3.database import Database
    db = Database(db_name)
    if not db.exists():
        db.create()
    else:
        db.open()
    return db


def get_main_content(url, silent=True):
    try:
        res = request.urlopen(url=url)
        return res.read()
    except URLError:
        if silent:
            return None
        raise  # TODO Log


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


def main_downloader():
    categories = parse_category(
        base_url=home_url,
        categories=extract_categories(
            html_content=get_main_content(home_url),
        ),
    )
    db = get_database()
    for category, cat_url in categories.items():
        print(f'Products from {category}')
        start = time.perf_counter()
        body = get_main_content(url=cat_url)
        if not body:
            print('error getting main content')
            continue
        pages = [cat_url]
        pages.extend(
            extract_pages(
                base_url=cat_url,
                content=body,
            )
        )
        end = time.perf_counter()
        print(f'Pages: {pages.__len__()} in {end - start}s')
        for cat_page in pages:
            start = time.perf_counter()
            list(map(
                lambda prod: db.insert(parse_product(prod)) if prod else None,
                extract_category_products(
                    category_html=get_main_content(
                        cat_page,
                    ),
                ),
            ))
            end = time.perf_counter()
        print(f'Products in {end - start}s')


if __name__ == '__main__':
    main_downloader()
