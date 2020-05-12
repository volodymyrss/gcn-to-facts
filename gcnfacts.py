import re
import sys
import json
from datetime import datetime
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


class NoSuchGCN(Exception):
    pass

class BoringGCN(Exception):
    pass

@cli.command()
@click.argument('gcnid', type=int)
@workflow
def _gcn_source(gcnid: int) -> str:  # -> gcn
    if True:
        try:
            t = open("gcn3/%i.gcn3" % gcnid, "rb").read().decode('ascii', 'replace')
            return t
        except FileNotFoundError:
            raise NoSuchGCN
    else:
        t = requests.get("https://gcn.gsfc.nasa.gov/gcn3/%i.gcn3" % gcnid).text
        #print(t)
        return t

def get_gcn_tag():
    print("https://gcn.gsfc.nasa.gov/gcn3/all_gcn_circulars.tar.gz")

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
    t = datetime.strptime(gcn_meta(gcntext)['DATE'], "%y/%m/%d %H:%M:%S GMT").timestamp()

    return dict(timestamp=t)

@workflow
def gcn_integral_lvc_countepart_search(gcntext: str):  # ->
    r = re.search("SUBJECT:(LIGO/Virgo.*?):.*INTEGRAL", gcntext, re.I)

    original_event = r.groups()[0].strip()

    return dict(original_event=original_event)


@workflow
def gcn_integral_countepart_search(gcntext: str):  # ->
    r = re.search("SUBJECT:(.*?):.*counterpart.*INTEGRAL", gcntext, re.I)

    original_event = r.groups()[0].strip()

    original_event_utc = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC, hereafter T0", gcntext).groups()[0]

    return dict(original_event=original_event, original_event_utc=original_event_utc)

@workflow
def gcn_icecube_circular(gcntext: str):  # ->
    r = re.search("SUBJECT:(.*?)- IceCube observation of a high-energy neutrino candidate event", 
                  gcntext, re.I).groups()[0].strip()

    return dict(reports_icecube_event=r)

@workflow
def gcn_lvc_circular(gcntext: str):  # ->
    r = re.search("SUBJECT:.*?(LIGO/Virgo .*?): Identification", 
                  gcntext, re.I).groups()[0].strip()

    return dict(lvc_event_report=r)

@workflow
def gcn_grb_integral_circular(gcntext: str):  # ->
    r = re.search("SUBJECT:.*?(GRB.*?):.*INTEGRAL.*", 
                  gcntext, re.I).groups()[0].strip()

    grbname = r
    
    grbtime = re.search("(\d\d:\d\d:\d\d) +UT", 
                  gcntext, re.I).groups()[0].strip()

    date=grbname.replace("GRB","").strip()
    utc = "20" + date[:2] + "-" + date[2:4] + "-" + date[4:6] + " " + grbtime

    return dict(integral_grb_report=grbname, event_t0=utc)

@workflow
def gcn_lvc_integral_counterpart(gcntext: str):  # ->
    r = re.search("SUBJECT:.*?(LIGO/Virgo .*?):.*INTEGRAL", 
                  gcntext, re.I).groups()[0].strip()

    return dict(lvc_counterpart_by = "INTEGRAL")

@workflow
def gcn_workflows(gcnid):
    gs = gcn_source(gcnid)

    #print(gs)
    
    G = rdflib.Graph()
    G.bind('gcn', rdflib.Namespace('http://odahub.io/ontology/gcn#'))

    for wn, w in workflow_context:
        print("..\n", Fore.BLUE+wn+Style.RESET_ALL, w)

        try:
            o = w(gs)

            print(Fore.GREEN+"found:"+Style.RESET_ALL, gcnid, wn, o)

            for k,v in o.items():
                if isinstance(v, float):
                    v="%.20lg"%v
                else:
                    v="\""+str(v)+"\""

                G.update('INSERT DATA {{ gcn:gcn{gcnid} gcn:{prop} {value} }}'.format(gcnid=gcnid, prop=k, value=v))

            print()

        except Exception as e:
            print(Fore.YELLOW+"problem"+Style.RESET_ALL, repr(e))

    #G.
    print("gcn", gcnid, "facts", len(list(G)))

    if len(list(G))<=3:
        raise BoringGCN

    return G.serialize(format='n3').decode()



@workflow
def gcns_workflows(gcnid1, gcnid2):
    G = rdflib.Graph()

    for gcnid in range(gcnid1, gcnid2):
        try:
            t = gcn_workflows(gcnid)
            G.parse(data=t, format="n3")
        except NoSuchGCN:
            print("no GCN %i"%gcnid)
        except BoringGCN:
            print("boring GCN %i"%gcnid)


    return G.serialize(format='n3').decode()

@cli.command()
def learn():
    t = gcns_workflows(17000, 28000)
    open("knowledge.n3", "w").write(t)

@cli.command()
@workflow
def contemplate():
    G = rdflib.Graph()

    G.parse("knowledge.n3", format="n3")

    print("parsed", len(list(G)))

    s = []

    for rep_gcn_prop in "gcn:lvc_event_report", "gcn:reports_icecube_event":
        for r in G.query("""
                    SELECT ?c ?ic_d ?ct_d ?t0 WHERE {{
                            ?ic_g {rep_gcn_prop} ?c . 
                            ?ct_g ?p ?c . 
                            ?ic_g gcn:DATE ?ic_d . 
                            ?ct_g gcn:DATE ?ct_d .
                            ?ct_g gcn:original_event_utc ?t0 .
                        }}
                """.format(rep_gcn_prop=rep_gcn_prop)):
            if r[1] != r[2]:
                print(r)
                s.append(dict(
                        event=str(r[0]),
                        event_gcn_time=str(r[1]),
                        counterpart_gcn_time=str(r[2]),
                        event_t0=str(r[3]),
                    ))

    json.dump(s, open("counterpart_gcn_reaction_summary.json", "w"))
    
    s = []
    for r in G.query("""
                    SELECT ?grb ?t0 ?gcn_d WHERE {{
                            ?gcn gcn:integral_grb_report ?grb . 
                            ?gcn gcn:DATE ?gcn_d . 
                            ?gcn gcn:event_t0 ?t0 .
                        }}
                """.format(rep_gcn_prop=rep_gcn_prop)):
        if r[1] != r[2]:
            print(r)
            s.append(dict(
                    event=str(r[0]),
                    event_t0=str(r[1]),
                    event_gcn_time=str(r[2]),
                ))
    json.dump(s, open("grb_gcn_reaction_summary.json", "w"))

if __name__ == "__main__":
    cli()
