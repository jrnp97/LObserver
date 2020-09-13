""" Main script """
import urllib.request as request
from bs4 import BeautifulSoup

from pprint import pprint

home_url = 'https://www.linio.com.co/'


def get_main_content(url):
    res = request.urlopen(url=url)
    return res.read()


def parse_categories(base_url, categories):
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


if __name__ == '__main__':
    categories = parse_categories(
        base_url=home_url,
        categories=extract_categories(
            html_content=get_main_content(home_url),
        ),
    )
    pprint(categories)
