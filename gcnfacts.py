import re
import sys
import requests
import click
from colorama import Fore, Back, Style

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
def gcn_source(gcnid: int) -> str:  # -> gcn
    t = requests.get("https://gcn.gsfc.nasa.gov/gcn3/%i.gcn3" % gcnid).text

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


@workflow
def gcn_instrument(gcntext: str):
    pass


@workflow
def gcn_meta(gcntext: str):  # ->
    d = {}

    for c in "DATE", "SUBJECT":
        d[c] = re.search(c+":(.*)", gcntext).groups()[0].strip()

    return d


@workflow
def gcn_date(gcntext: str) -> float:  # date
    gcn_meta(gcntext)

    return dict(original_event=original_event)


@workflow
def gcn_countepart_search(gcntext: str):  # ->
    r = re.search("SUBJECT:(.*?):.*counterpart", gcntext, re.I)

    original_event = r.groups()[0].strip()

    r = re.search("DATE:(.*?):.*counterpart", gcntext, re.I)

    return dict(original_event=original_event)


@cli.command()
@click.argument('gcnid', type=int)
def gcn_workflows(gcnid):
    gs = _gcn_source(gcnid)

    print(gs)

    for wn, w in workflow_context:
        print("..\n", Fore.BLUE+wn+Style.RESET_ALL, w)

        try:
            o = w(gs)

            print(Fore.GREEN+"found:"+Style.RESET_ALL, gcnid, wn, o)
        except Exception as e:
            print(Fore.YELLOW+"problem"+Style.RESET_ALL, repr(e))


if __name__ == "__main__":
    cli()
