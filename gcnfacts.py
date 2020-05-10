import re
import sys
import requests
import click

workflow_context = []

def workflow(f):
    setattr(sys.modules[f.__module__], '_' + f.__name__, f)
    workflow_context.append((f.__name__, f))
    return f

@click.group()
def cli():
    pass

@cli.command()
@click.argument('gcnid', type=int)
@workflow
def gcn_source(gcnid: int) -> str:
    t = requests.get("https://gcn.gsfc.nasa.gov/gcn3/%i.gcn3"%gcnid).text

    print(t)

    return t

@cli.command()
@workflow
def gcn_list_recent():
    gt = requests.get("https://gcn.gsfc.nasa.gov/gcn3_archive.html").text

    r = re.findall(r"<A HREF=(gcn3/\d{1,5}.gcn3)>(\d{1,5})</A>", gt)

    print("results", len(r))

    for u, i in reversed(r):
        print(u, i)




if __name__ == "__main__":
    cli()
