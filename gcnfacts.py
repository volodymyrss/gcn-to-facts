import re
import sys
import requests
import click
import rdflib
from colorama import Fore, Back, Style

workflow_context = []


def workflow(f):
    setattr(sys.modules[f.__module__], f.__name__[1:], f)
    workflow_context.append((f.__name__, f))
    return f


@click.group()
def cli():
    pass


@cli.command()
@click.argument('gcnid', type=int)
@workflow
def _gcn_source(gcnid: int) -> str:  # -> gcn
    t = requests.get("https://gcn.gsfc.nasa.gov/gcn3/%i.gcn3" % gcnid).text

    #print(t)

    return t


@cli.command()
@workflow
def _gcn_list_recent():
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

    original_event_utc = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC, hereafter T0", gcntext).groups()[0]

    return dict(original_event=original_event, original_event_utc=original_event_utc)

@workflow
def gcn_icecube_circular(gcntext: str):  # ->
    r = re.search("SUBJECT:(.*?)- IceCube observation of a high-energy neutrino candidate event", 
                  gcntext, re.I).groups()[0].strip()

    return dict(reports_icecube_event=r)


@cli.command()
@click.argument('gcnid', type=int)
@workflow
def _gcn_workflows(gcnid):
    gs = gcn_source(gcnid)

    print(gs)
    
    G = rdflib.Graph()
    G.bind('gcn', rdflib.Namespace('http://odahub.io/ontology/gcn#'))

    for wn, w in workflow_context:
        print("..\n", Fore.BLUE+wn+Style.RESET_ALL, w)

        try:
            o = w(gs)

            print(Fore.GREEN+"found:"+Style.RESET_ALL, gcnid, wn, o)

            for k,v in o.items():
                G.update('INSERT DATA {{ gcn:gcn{gcnid} gcn:{prop} "{value}" }}'.format(gcnid=gcnid, prop=k, value=v))

            print()

        except Exception as e:
            print(Fore.YELLOW+"problem"+Style.RESET_ALL, repr(e))

    return G.serialize(format='n3').decode()

@workflow
def counterpart_sequence():
    pass


if __name__ == "__main__":
    cli()
