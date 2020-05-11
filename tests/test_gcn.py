def test_icecube():
    import gcnfacts as gf

    mt = gf.gcn_meta(
                gf.gcn_source(27619)
            )

    print(mt)
