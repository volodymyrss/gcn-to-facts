from concurrent import futures
import re
import sys
import json
from datetime import datetime
import requests
import click
import rdflib
from colorama import Fore, Back, Style

import logging
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger()

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


@workflow
def gcn_source(gcnid: int) -> str:  # -> gcn
    if True:
        try:
            t = open("gcn3/%i.gcn3" %
                     gcnid, "rb").read().decode('ascii', 'replace')
            return t
        except FileNotFoundError:
            raise NoSuchGCN
    else:
        t = requests.get("https://gcn.gsfc.nasa.gov/gcn3/%i.gcn3" % gcnid).text
        return t


def get_gcn_tag():
    logger.debug("https://gcn.gsfc.nasa.gov/gcn3/all_gcn_circulars.tar.gz")


@cli.command()
@workflow
def _gcn_list_recent():
    gt = requests.get("https://gcn.gsfc.nasa.gov/gcn3_archive.html").text

    r = re.findall(r"<A HREF=(gcn3/\d{1,5}.gcn3)>(\d{1,5})</A>", gt)

    logger.debug(f"results {len(r)}")

    for u, i in reversed(r):
        logger.debug(f"{u} {i}")


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
    t = datetime.strptime(
        gcn_meta(gcntext)['DATE'], "%y/%m/%d %H:%M:%S GMT").timestamp()

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

    original_event_utc = re.search(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC, hereafter T0", gcntext).groups()[0]

    instruments = []
    if re.search("SUBJECT:(.*?):.*ACS.*", gcntext, re.I):
        instruments.append("acs")

    if re.search("SUBJECT:(.*?):.*IBIS.*", gcntext, re.I):
        instruments.append("ibis")

    return dict(
        original_event=original_event,
        original_event_utc=original_event_utc,
        instrument=instruments,
    )


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

    grbtime = re.search(r"(\d\d:\d\d:\d\d) +UT",
                        gcntext, re.I).groups()[0].strip()

    date = grbname.replace("GRB", "").strip()
    utc = "20" + date[:2] + "-" + date[2:4] + "-" + date[4:6] + " " + grbtime

    return dict(integral_grb_report=grbname, event_t0=utc)


@workflow
def gcn_lvc_integral_counterpart(gcntext: str):  # ->
    r = re.search("SUBJECT:.*?(LIGO/Virgo .*?):.*INTEGRAL",
                  gcntext, re.I).groups()[0].strip()

    return dict(lvc_counterpart_by="INTEGRAL")


@workflow
def gcn_workflows(gcnid):
    gs = gcn_source(gcnid)

    gcn_ns = 'http://odahub.io/ontology/gcn#'

    facts = []

    for wn, w in workflow_context:
        logger.debug(f".. {Fore.BLUE} {wn} {Style.RESET_ALL} {w}")

        try:
            o = w(gs)

            logger.debug(
                f"{Fore.GREEN} found:  {Style.RESET_ALL} {gcnid} {wn} {o}")

            for k, v in o.items():
                if isinstance(v, list):
                    vs = v
                else:
                    vs = [v]

                for _v in vs:
                    if isinstance(_v, float):
                        _v = "%.20lg" % _v
                    else:
                        _v = "\""+str(_v)+"\""

                    data = '<{gcn_ns}gcn{gcnid}> <{gcn_ns}{prop}> {value}'.format(
                        gcn_ns=gcn_ns, gcnid=gcnid, prop=k, value=_v
                    )

                    facts.append(data)

                    #G.update('INSERT DATA { '+data+' }')

        except Exception as e:
            logger.debug(f"{Fore.YELLOW} problem {Style.RESET_ALL} {repr(e)}")

    if len(list(facts)) <= 3:
        raise BoringGCN

    logger.info(f"gcn {gcnid} facts {len(facts)}")

    return facts


def gcns_workflows(gcnid1, gcnid2, nthreads=1):
    G = rdflib.Graph()

    def run_one_gcn(gcnid):
        try:
            return gcnid, gcn_workflows(gcnid)
        except NoSuchGCN:
            logger.debug(f"no GCN {gcnid}")
        except BoringGCN:
            logger.debug(f"boring GCN {gcnid}")

        return gcnid, ""

    with futures.ThreadPoolExecutor(max_workers=nthreads) as ex:
        for gcnid, d in ex.map(run_one_gcn, range(gcnid1, gcnid2)):
            logger.debug(f"{gcnid} gives: {len(d)}")
            for s in d:
                G.update(f'INSERT DATA {{ {s} }}')

    return G.serialize(format='n3').decode()


@cli.command()
@click.option("--from-gcnid", "-f", default=1500)
@click.option("--to-gcnid", "-t", default=30000)
@click.option("--workers", "-w", default=1)
def learn(from_gcnid, to_gcnid, workers):
    t = gcns_workflows(from_gcnid, to_gcnid, workers)

    logger.info("read in total %i", len(t))

    open("knowledge.n3", "w").write(t)


@cli.command()
def contemplate():
    G = rdflib.Graph()

    G.parse("knowledge.n3", format="n3")

    logger.info(f"parsed {len(list(G))}")

    s = []

    for rep_gcn_prop in "gcn:lvc_event_report", "gcn:reports_icecube_event":
        for r in G.query("""
                    SELECT ?c ?ic_d ?ct_d ?t0 ?instr WHERE {{
                            ?ic_g {rep_gcn_prop} ?c;
                                  gcn:DATE ?ic_d . 
                            ?ct_g ?p ?c;
                                  gcn:DATE ?ct_d;
                                  gcn:original_event_utc ?t0;
                                  gcn:instrument ?instr .
                        }}
                """.format(rep_gcn_prop=rep_gcn_prop)):

            if r[1] != r[2]:
                logger.debug(r)
                s.append(dict(
                    event=str(r[0]),
                    event_gcn_time=str(r[1]),
                    counterpart_gcn_time=str(r[2]),
                    event_t0=str(r[3]),
                    instrument=str(r[4]),
                ))

    byevent = dict()

    for i in s:
        ev = i['event']
        if ev in byevent:
            byevent[ev]['instrument'].append(i['instrument'])
        else:
            byevent[ev] = i
            byevent[ev]['instrument'] = [i['instrument']]

    s = list(byevent.values())

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
            logger.debug(r)
            s.append(dict(
                event=str(r[0]),
                event_t0=str(r[1]),
                event_gcn_time=str(r[2]),
            ))
    json.dump(s, open("grb_gcn_reaction_summary.json", "w"))


if __name__ == "__main__":
    cli()
