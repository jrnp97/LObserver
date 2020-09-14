""" Main script """
import re
import urllib.request as request

from bs4 import BeautifulSoup

from pprint import pprint

home_url = 'https://www.linio.com.co/'


def get_main_content(url):
    res = request.urlopen(url=url)
    return res.read()


def parse_category(base_url, categories):
    return {
        cat.attrs['title']: f'{base_url[:-1]}{cat.attrs["href"]}' for cat in categories
    }


def extract_categories(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    nav_bar = soup.findAll('nav', {'itemtype': 'http://www.schema.org/SiteNavigationElement'})
    if not nav_bar:
        raise ValueError('HTML content is not valid to extract categories.')
    if nav_bar.__len__() > 1:
        raise ValueError('Verify HTML content return unexpected results.')
    nav_bar = nav_bar[0]
    return nav_bar.findAll('a')


def parse_product(product_div):
    return {
        meta.attrs['itemprop']: meta.attrs['content'] for meta in product_div.findAll('meta')
    }


def extract_category_products(category_html):
    parser = BeautifulSoup(category_html, 'html.parser')
    products = parser.find('div', {'id': 'catalogue-product-container'})
    for product in products.findAll('div', {'itemtype': 'http://schema.org/Product'}):
        yield product


def extract_pages(content):
    rgx = re.compile(r'\d+')
    pages_link = content.findAll('li', {'class': 'page-item'})
    if not pages_link:
        return []

    def get_link(li_):
        return li_.find('a').attrs['href']

    max_page = max(
        map(lambda link: int(rgx.findall(get_link(link))[0]) if rgx.findall(get_link(link)) else -1, pages_link)
    )
    return [f'?page={page}' for page in range(2, max_page + 1)]


if __name__ == '__main__':
    categories = parse_category(
        base_url=home_url,
        categories=extract_categories(
            html_content=get_main_content(home_url),
        ),
    )
    for category, cat_url in categories.items():
        print(f'Products from {category}')
        body = get_main_content(cat_url)
        parser = BeautifulSoup(body, 'html.parser')
        pages = [cat_url]
        pages.extend([f'{cat_url}{page}' for page in extract_pages(parser)])
        print(pages)
        for cat_page in pages:
            pprint(
                [parse_product(prod) for prod in extract_category_products(category_html=get_main_content(cat_page))]
            )
            break
        break