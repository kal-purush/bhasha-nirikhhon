def filterType(x):
    if isinstance(x, int):
        if x>=100:
            print 'That is a big number'
        if x<100:
            print 'That is a small number'
    if isinstance(x, str):
        if len(x) >=50:
            print 'That is a long sentence'
        if len(x) <50:
            print 'That is a short sentence'
    if isinstance(x, list):
        if len(x) >=10:
            print 'It is a big list'
        if len(x) <10:
            print 'It is  short list'