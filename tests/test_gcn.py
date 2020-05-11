def test_icecube_counterpart_antares():
    import gcnfacts as gf

    mt = gf.gcn_meta(
                gf.gcn_source(27619)
            )

    print(mt)

def test_icecube_counterpart_integral():
    import gcnfacts as gf

    gcnid = 27652

    mt = gf.gcn_meta(
                gf.gcn_source(gcnid)
            )

    print(mt)
    
    ct = gf.gcn_countepart_search(
                gf.gcn_source(gcnid)
            )

    print(ct)

    #icecube itself
    
    ic = gf.gcn_icecube_circular(
                gf.gcn_source(27651)
            )

    print(ic)
    
def test_workflows():
    import gcnfacts as gf
    import rdflib

    G = rdflib.Graph()

    t = gf.gcn_workflows(27652) # integral

    G.parse(data=t, format="n3")

    t = gf.gcn_workflows(27651) # icecube

    G.parse(data=t, format="n3")

    print(G.serialize(format='n3').decode())

