# Silpo stores

Automatically create CSV out of Silpo store information available at https://silpo.ua/stores.html.

## Manual usage

Set up local virtual env using [poetry](https://python-poetry.org/docs/):

`poetry install`

Parse the website and output some stats to stdout:

`poetry run silpostores parse_url`

Parse the website and update the specified output CSV file:

`poetry run silpostores --output-file data/silpo-stores.csv parse_url`