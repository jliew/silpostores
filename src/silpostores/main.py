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

        adm4_cols = [
            'admin4Name_en', 'admin4Name_ua', 'admin4Pcode',
            'admin3Name_en', 'admin3Name_ua', 'admin3Pcode',
            'admin2Name_en', 'admin2Name_ua', 'admin2Pcode',
            'admin1Name_en', 'admin1Name_ua', 'admin1Pcode'
            ]

        # manual mapping overrides
        manual_df = pd.read_csv(pathlib.Path().cwd() / 'src' / 'silpostores' / 'seeds' / 'manual-mapping-overrides.csv', encoding='utf-8')
        (manual_matched_df, manual_remaining_df) = map_pcodes(df, manual_df, adm4_cols, left_on='title', right_on='titleManual',
            duplicated_col_name='duplicated_manual')
        manual_matched_df = manual_matched_df.drop(['cityTitleManual', 'storeTitleManual', 'titleManual'], axis=1)

        # get ocha xlsx
        ocha_file = pathlib.Path().cwd() / 'src' / 'silpostores' / 'seeds' / 'ukr_adminboundaries_tabulardata.xlsx'

        # join on admin4Name_ua
        adm4_df = pd.read_excel(ocha_file, sheet_name='Admin4')
        adm4_df = adm4_df[adm4_cols]
        (adm4_matched_df, adm4_remaining_df) = map_pcodes(manual_remaining_df, adm4_df, adm4_cols)

        # join on admin3Name_ua
        adm3_cols = [
            'admin3Name_en', 'admin3Name_ua', 'admin3Pcode',
            'admin2Name_en', 'admin2Name_ua', 'admin2Pcode',
            'admin1Name_en', 'admin1Name_ua', 'admin1Pcode'
            ]
        adm3_df = pd.read_excel(ocha_file, sheet_name='Admin3')
        adm3_df = adm3_df[adm3_cols]
        (adm3_matched_df, adm3_remaining_df) = map_pcodes(adm4_remaining_df, adm3_df, adm3_cols,
            right_on='admin3Name_ua', duplicated_col_name='duplicated_adm3')

        # join on admin2Name_ua
        adm2_cols = [
            'admin2Name_en', 'admin2Name_ua', 'admin2Pcode',
            'admin1Name_en', 'admin1Name_ua', 'admin1Pcode'
            ]
        adm2_df = pd.read_excel(ocha_file, sheet_name='Admin2')
        adm2_df = adm2_df[adm2_cols]
        (adm2_matched_df, adm2_remaining_df) = map_pcodes(adm3_remaining_df, adm2_df, adm2_cols,
            right_on='admin2Name_ua', duplicated_col_name='duplicated_adm2')

        # concat results
        result_df = pd.concat([ manual_matched_df, adm4_matched_df, adm3_matched_df, adm2_matched_df ])
        click.echo(f"final matched: {result_df.shape}, remaining: {adm2_remaining_df.shape}")
        
        if ctx.obj['OUTPUT_FILE']:
            create_data_file(ctx.obj['OUTPUT_FILE'], pd.concat([ result_df, adm2_remaining_df ]))


def map_pcodes(left_df, right_df, cols, left_on='cityTitle', right_on='admin4Name_ua',
        duplicated_col_name='duplicated_adm4', unique_col_name='title', duplicated_keep='first'):
    """Map pcodes on the right with admin names on the left.

    duplicated_keep: passed to duplicated(). Set to False to 'reject' all ambiguous matches by nullifying
    mapped fields.
    """

    click.echo("---------------")
    click.echo(f"joining left: {left_df.shape} with right: {right_df.shape}")
    # match on adm3, adm2 better by normalising gender suffixes
    right_df[f"{right_on}_normalised"] = right_df[right_on].str.replace("ська", "").replace("ський", "")
    left_df[f"{left_on}_normalised"] = left_df[left_on].str.replace("ська", "").replace("ський", "")
    merged_df = left_df.merge(right_df, how='left', left_on=f"{left_on}_normalised", right_on=f"{right_on}_normalised")
    merged_df = merged_df.drop(f"{right_on}_normalised", axis=1)
    merged_df = merged_df.drop(f"{left_on}_normalised", axis=1)
    left_df = left_df.drop(f"{left_on}_normalised", axis=1)

    # nullify shops which could be in multiple cities
    merged_df[duplicated_col_name] = merged_df.duplicated(unique_col_name, keep=duplicated_keep)
    for col in cols:
        merged_df.loc[merged_df[duplicated_col_name], col] = None
    merged_df = merged_df.drop(duplicated_col_name, axis=1)

    # remove results of multiple matches
    merged_df = merged_df.drop_duplicates(unique_col_name)

    click.echo(f"merged_df.shape: {merged_df.shape}")

    # matched
    matched_df = merged_df[(merged_df[right_on].notna())]
    remaining_df = merged_df[(merged_df[right_on].isna())]
    remaining_df = remaining_df[left_df.columns]
    click.echo(f"matched_df.shape: {matched_df.shape}, remaining_df.shape: {remaining_df.shape}")
    
    return (matched_df, remaining_df)


@cli.command("parse_silpo_shops_mapping")
@click.pass_context
def parse_silpo_shops_mapping(ctx):
    """Parse the locations seed file.
    """

    df = pd.read_csv(r'src\silpostores\seeds\silpo-shops-mapping.csv')
    debug_df(df)
