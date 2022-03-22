import json
import pathlib
from datetime import datetime
from urllib.request import Request, urlopen

import click
import pandas as pd

from silpostores.parser.stores import parse_html


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


def debug_df(df):
    """Print DataFrame info."""

    click.echo(df)
    click.echo(df.info())


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

        # normalise/clean apostrophe character in order to join with ocha values
        df['cityTitle'] = df['cityTitle'].str.replace('’', '\'')

        # get ocha xlsx
        ocha_cols = [
            'admin4Name_en', 'admin4Name_ua', 'admin4Name_ru', 'admin4Pcode',
            'admin3Name_en', 'admin3Name_ua', 'admin3Name_ru', 'admin3Pcode',
            'admin2Name_en', 'admin2Name_ua', 'admin2Name_ru', 'admin2Pcode',
            'admin1Name_en', 'admin1Name_ua', 'admin1Name_ru', 'admin1Pcode'
            ]
        ocha_df = pd.read_excel(pathlib.Path().cwd() / 'src' / 'silpostores' / 'seeds' / 'ukr_adminboundaries_tabulardata.xlsx', sheet_name='Admin4')
        ocha_df = ocha_df[ocha_cols]

        # join on admin4Name_ua
        merged_df = df.merge(ocha_df, how='left', left_on='cityTitle', right_on='admin4Name_ua')

        # nullify shops which could be in multiple cities
        merged_df['duplicated'] = merged_df.duplicated('title', keep=False)
        merged_df = merged_df.drop_duplicates('title')
        for col in ocha_cols:
            merged_df.loc[merged_df['duplicated'], col] = None

        debug_df(merged_df)
        
        if ctx.obj['OUTPUT_FILE']:
            create_data_file(ctx.obj['OUTPUT_FILE'], merged_df)


@cli.command("parse_silpo_shops_mapping")
@click.pass_context
def parse_silpo_shops_mapping(ctx):
    """Parse the locations seed file.
    """

    df = pd.read_csv(r'src\silpostores\seeds\silpo-shops-mapping.csv')
    debug_df(df)
