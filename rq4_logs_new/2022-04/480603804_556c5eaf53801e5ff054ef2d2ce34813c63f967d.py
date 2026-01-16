import click

from analyzer import *
from downloader import *
from utils import *


@click.group()
@click.version_option(version='1.0.0')
def cli():
    pass


@cli.command()
@click.option('-a', '--append', is_flag=True, default=False)
@click.option('-e', '--epoch', default=1)
@click.option('-f', '--filename', default="../resources/data.txt")
def download(append, epoch, filename):
    DataDownloader(sleep_time=0).download_matches(filename, mode='a' if append else 'w', epoch=epoch)


@cli.command()
@click.option('-a', '--all', is_flag=True, default=False)
@click.option('-h', '--hero', help='heroid or heroname')
@click.option('-f', '--filename', default="../resources/data.txt")
def winrate(all, hero, filename):
    az = Analyzer(filename)
    if all:
        print(az.get_all_winrate().to_string())
        return
    if not hero:
        ctx = click.get_current_context()
        ctx.fail("hero is required")
    hid = int(hero) if hero.isdigit() else guess_hero_id(hero)
    if not hid:
        print("can't find this hero, try another name")
        return
    hdict = get_heroid_dict()
    print(hdict[hid], az.get_hero_winrate(hid).to_string())


@cli.command()
@click.option('-a', '--all', is_flag=True, default=False)
@click.option('-h', '--hero', help='heroid or heroname')
@click.option('-f', '--filename', default="../resources/data.txt")
def counter(all, hero, filename):
    az = Analyzer(filename)
    if all:
        print(az.get_all_counter_winrate().to_string())
        return
    if not hero:
        ctx = click.get_current_context()
        ctx.fail("hero is required")
    hid = int(hero) if hero.isdigit() else guess_hero_id(hero)
    if not hid:
        print("can't find this hero, try another name")
        return
    hdict = get_heroid_dict()
    print(hdict[hid], az.get_counter_winrate(hid).to_string())


if __name__ == "__main__":
    cli()