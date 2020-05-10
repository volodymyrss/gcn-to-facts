import re
import requests
import click

@click.group()
def cli():
    pass

@cli.command()
@click.argument('gcnid', type=int)
def gcn_source(gcnid):
    t = requests.get("https://gcn.gsfc.nasa.gov/gcn3/%i.gcn3"%gcnid).text

    print(t)

@cli.command()
def gcn_list_recent():
    gt = requests.get("https://gcn.gsfc.nasa.gov/gcn3_archive.html").text

    r = re.findall("<A HREF=(gcn3/\d{1,5}.gcn3)>(\d{1,5})</A>", gt)

    print("results", len(r))

    for u, i in reversed(r):
        print(u, i)


if __name__ == "__main__":
    cli()
