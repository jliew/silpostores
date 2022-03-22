import click
import pandas as pd
from bs4 import BeautifulSoup


def find_data(soup):
    """Parse div rows containing div columns, put data into DataFrame.
    """

    shops = []
    for row in soup.find_all('div', attrs={ 'class': 'row' }):
        shop = {}
        for col in row.find_all('div'):
            if 'col-city' in col['class']:
                shop['city'] = col.text
            elif 'col-store' in col['class']:
                shop['store'] = col.text
            elif 'col-working' in col['class']:
                shop['working'] = col.text
            elif 'col-terminal' in col['class']:
                img = col.find('img')
                if img and 'check' in img['alt']:
                    shop['payment_by_card'] = True
                elif img and 'cancel' in img['alt']:
                    shop['payment_by_card'] = False
            elif 'col-money' in col['class']:
                img = col.find('img')
                if img and 'check' in img['alt']:
                    shop['cash_withdrawal'] = True
                elif img and 'cancel' in img['alt']:
                    shop['cash_withdrawal'] = False
        shops.append(shop)
    
    return pd.DataFrame(shops)


def parse_html(html):
    """Parse data from string containing HTML.

    Returns a DataFrame.
    """

    soup = BeautifulSoup(html, 'html.parser')
    return find_data(soup)
