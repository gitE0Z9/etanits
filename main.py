import click

from scraper.scraper import Scraper
from app.app import app
from analyze.report import Reporter


# data
@click.group()
def data():
    ...


@data.command()
@click.option(
    "--rootDirectory",
    "rootDirectory",
    default="data",
    help="root directory for downloaded data.",
)
@click.option(
    "--season",
    required=True,
    help="Year and season e.g. 108S2.",
)
@click.option(
    "--regionCode",
    "regionCode",
    required=True,
    help="Region code e.g. A for Taipei city. For multiple cities, use comman separated string.",
)
def download(rootDirectory: str, season: str, regionCode: str):
    scraper = Scraper(rootDirectory, season, regionCode)
    scraper.run()


# api
@click.group()
def server():
    ...


@server.command()
@click.option(
    "--production",
    is_flag=True,
    help="Production mode on or off.",
)
def start(production: bool):
    app.run(
        debug=not production,
        host="0.0.0.0",
    )


# analyze
@click.group()
def analyze():
    ...


@analyze.command()
@click.option(
    "--src",
    default="data",
    help="Source directory.",
)
@click.option(
    "--dst",
    default="result",
    help="Destination directory.",
)
def report(src: str, dst: str):
    reporter = Reporter(src, dst)
    reporter.run()


cli = click.CommandCollection(sources=[data, server, analyze])


if __name__ == "__main__":
    cli()
