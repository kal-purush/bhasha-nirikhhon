import math

def mlt_percent(list_percentage, factor=100):
    return [p * factor for p in list_percentage]

def div_percent(list_percentage, factor=100):
    return [p / factor for p in list_percentage]

def abserr_to_relerr(list):
    return [mlt_percent([abserr / val])[0] for val,abserr in list]

def relerr_to_abserr(list):
    return [val * div_percent([relerr])[0] for val,relerr in list]

def abspp(list):
    return math.sqrt(sum([abserr ** 2 for abserr in list]))

def relpp(list):
    return mlt_percent([math.sqrt(sum([relerr ** 2 for relerr in div_percent(list)]))])[0]

def abspp_rel(res, list):
    return abserr_to_relerr([(res, abspp(relerr_to_abserr(list)))])[0]

def relpp_abs(res, list):
    return relerr_to_abserr([(res, relpp(abserr_to_relerr(list)))])[0]