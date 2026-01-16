from fits_reduce.util.slices import slices_config

def test_slices():
    assert slices_config('[0:10, 20:30], [30, 40:50], [13::], [::-1]') == [(slice(0, 10, None), slice(20, 30, None)),
                                                                           (slice(None, 30, None), slice(40, 50, None)),
                                                                           (slice(13, None, None),),
                                                                           (slice(None, None, -1),)]