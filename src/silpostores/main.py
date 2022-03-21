import json
import pathlib
from datetime import datetime
from urllib.request import Request, urlopen

import click
import pandas as pd

from silpostores.parser.stores import debug_df, parse_html


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--output-file', type=click.STRING, help='Output data file to append to.')
@click.pass_context
def cli(ctx, debug, output_file):
    """Crawler for https://silpo.ua/stores.html.

    OUTPUT_FILE is the path to an output CSV file for the crawled provincial case numbers.
    No-op when omitted.
    """

    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug
    ctx.obj['OUTPUT_FILE'] = output_file


def read_file(html_file):
    """Read file.
    
    Returns contents of file as text.
    """

    f = pathlib.Path(html_file)

    if not f.exists():
        click.echo(f"{f} does not exist.")

    return f.read_text(encoding='utf-8')


def create_data_file(output_file, new_df):
    """Create new CSV file."""

    # Create new file
    f = pathlib.Path(output_file)
    f = f.parent / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{f.name}"
    new_df.to_csv(f, index=False)
    click.echo(f"Created new CSV file {f}.")
    return


@cli.command("parse_file")
@click.argument('html_file', type=click.STRING, required=True)
@click.pass_context
def parse_file(ctx, html_file):
    """Parse the given HTML file representation of the https://silpo.ua/stores.html website.

    HTML_FILE is the path to the file.
    """

    html = read_file(html_file)
    df = parse_html(html)
    debug_df(df)

    if ctx.obj['OUTPUT_FILE']:
        create_data_file(ctx.obj['OUTPUT_FILE'], df)


@cli.command("parse_url")
@click.pass_context
def parse_url(ctx, url='https://silpo.ua/graphql'):
    """Parse the https://silpo.ua/graphql endpoint.
    """

    req = Request(url, bytes(json.dumps({ 
        'operationName': 'storesActivity',
        'query': 'query storesActivity {\n  storesActivity{\n  cityTitle\n storeTitle\n   title\n  activityTimeRange\n    cacheAmount\n    terminalEnabled\n    }\n}\n'
        }), encoding='utf-8'))
    with urlopen(req) as res:
        body = json.load(res)
        df = pd.DataFrame(body['data']['storesActivity'])
        debug_df(df)

        mapping_df = pd.read_csv(pathlib.Path().cwd() / 'src' / 'silpostores' / 'seeds' / 'silpo-shops-mapping.csv')
        df = df.merge(mapping_df, how='left', left_on='cityTitle', right_on='city_UKR')
        debug_df(df)
        
        if ctx.obj['OUTPUT_FILE']:
            create_data_file(ctx.obj['OUTPUT_FILE'], df)


@cli.command("parse_silpo_shops_mapping")
@click.pass_context
def parse_silpo_shops_mapping(ctx):
    """Parse the locations seed file.
    """

    df = pd.read_csv(r'src\silpostores\seeds\silpo-shops-mapping.csv')
    debug_df(df)
