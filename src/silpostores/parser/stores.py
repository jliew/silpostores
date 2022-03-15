import pathlib
from datetime import datetime

import click
import pandas as pd
from bs4 import BeautifulSoup


def debug_df(df):
    """Print DataFrame info."""

    click.echo(df)
    click.echo(df.info())


def update_data_file(output_file, new_df):
    """Append new data to existing data file if doesn't already exist."""

    # Create new file
    f = pathlib.Path(output_file)
    f = f.parent / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{f.name}"
    new_df.to_csv(f, index=False)
    click.echo(f"Created new CSV file {f}.")
    return


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
