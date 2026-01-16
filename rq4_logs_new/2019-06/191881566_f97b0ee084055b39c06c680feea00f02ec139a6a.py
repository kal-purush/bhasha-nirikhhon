import collections
import decimal
from functools import partial

SING   = 0
BOTH   = 1
SINGOT = 2
BOTHOT = 3
CENTS = decimal.Decimal('0.01')

def sr(index, hours, rates):
    return hours * rates[index]

def srh(index, hours, rates):
    return (sr(index, hours, rates)/decimal.Decimal(2)).quantize(CENTS)

def hr(hours, rates):
    return (min(hours,8) * rates[BOTH]) + (max(hours-8,0) * rates[SING])

def hrh(hours, rates):
    return (hr(hours, rates)/decimal.Decimal(2)).quantize(CENTS)

def nanny_calculate(sconfig, periods, taxtables, name, ndata):

    ret = dict()
    calcs = list()

    for child in sconfig.children:
        #             Input Hours Key,  Rate Function,        Destination Keys
        calcs.append(   ('Both',        partial(srh, BOTH),   [child + ' Gross', child + ' Both']))
        calcs.append(   ('Both OT',     partial(srh, BOTHOT), [child + ' Gross', child + ' Both OT']))
        calcs.append(   ('Sick',        hrh,                  [child + ' Gross', child + ' Sick']))
        calcs.append(   ('Holiday',     hrh,                  [child + ' Gross', child + ' Holiday']))
        calcs.append(   (child,         partial(sr, SING),    [child + ' Gross', child]))
        calcs.append(   (child + ' OT', partial(sr, SINGOT),  [child + ' Gross', child + ' OT']))


    for period in periods:
        start = period.startDate()
        end   = period.endDate()
        rates = period.rates(name)

        ret[end] = dict()
        p = ret[end]['hours'] = collections.defaultdict(decimal.Decimal)
        s = ret[end]['sums'] = collections.defaultdict(decimal.Decimal)

        for h in ndata.hours:
            if h.date > end: break

            # Full YTD calculations
            for hrkey, ratefunc, dkeys in calcs:
                for dkey in dkeys:
                    p[dkey + ' YTD'] += h.hours(hrkey)
                    s[dkey + ' YTD'] += ratefunc(h.hours(hrkey), rates)

            if h.date < start: continue

            # Just current period calculations
            for hrkey, ratefunc, dkeys in calcs:
                for dkey in dkeys:
                    p[dkey] += h.hours(hrkey)
                    s[dkey] += ratefunc(h.hours(hrkey), rates)

        for r in ndata.reimbursements:
            if r.date > end: break
            # Full YTD calculations
            for child in sconfig.children:
                s[child+' Reimbursements YTD'] += r.amount/2

            if r.date < start: continue
            for child in sconfig.children:
                s[child+' Reimbursements'] += r.amount/2

        for child in sconfig.children:
            for key in (child+' Reimbursements', child+' Reimbursements YTD'):
                s[key] = s[key].quantize(CENTS, rounding=decimal.ROUND_UP)


    ## Taxes and net
    ytd = collections.defaultdict(decimal.Decimal)
    for period in periods:
        start = period.startDate()
        end   = period.endDate()
        rates = period.rates(name)

        sums = ret[end]
        w4   = period.withholding(name)
        s    = sums['sums']
        t    = sums['tax'] = collections.defaultdict(decimal.Decimal)
        n    = sums['net'] = collections.defaultdict(decimal.Decimal)

        totalgross = decimal.Decimal(0)
        for child in sconfig.children:
            totalgross += s[child+' Gross']
        fed = taxtables.getTax(w4[0], w4[1], w4[2], totalgross)  # fed is calculated as 1 and then divided between employers

        for child in sconfig.children:
            futagross = wagross = childgross = s[child+' Gross']
            fedtax = 0

            # Limit for FUTA per year
            if s[child+' Gross YTD'] > sconfig.fed_unemployment_base:
                futagross = max(0, futagross - (s[child+' Gross YTD'] - sconfig.fed_unemployment_base))
                print("futagross = {} {}".format(end, futagross))

            # Limit for WA taxes per year
            if s[child+' Gross YTD'] > sconfig.wa_wage_base:
                wagross   = max(0, wagross   - (s[child+' Gross YTD'] - sconfig.wa_wage_base))

            if totalgross: fedtax = ((childgross / totalgross) * fed).quantize(CENTS)
            ss       = ((childgross * sconfig.social_security)/2).quantize(CENTS)
            medicare = ((childgross * sconfig.medicare)/2).quantize(CENTS)
            fedunemp =  (futagross  * sconfig.fed_unemployment).quantize(CENTS)
            waleave  =  (wagross    * sconfig.family_leave).quantize(CENTS)
            waunemp  =  (wagross    * sconfig.wa_unemployment).quantize(CENTS)

            employee = fedtax   + ss + medicare + waleave 
            employer = fedunemp + ss + medicare + waunemp

            # rolling count
            ytd[child+' Fed']       += fedtax
            ytd[child+' SS1']       += ss
            ytd[child+' SS2']       += ss
            ytd[child+' Medicare1'] += medicare
            ytd[child+' Medicare2'] += medicare
            ytd[child+' WALeave']  += waleave
            ytd[child+' WAUnemp']  += waunemp
            ytd[child+' FedUnemp'] += fedunemp
            ytd[child+' EmployeeTax'] += employee
            ytd[child+' EmployerTax'] += employer

            # child
            t[child+' Fed']       = fedtax
            t[child+' SS1']       = ss
            t[child+' SS2']       = ss
            t[child+' Medicare1'] = medicare
            t[child+' Medicare2'] = medicare
            t[child+' WALeave']   = waleave
            t[child+' WAUnemp']   = waunemp
            t[child+' FedUnemp']  = fedunemp
            t[child+' EmployeeTax'] = employee
            t[child+' EmployerTax'] = employer
            for copy in ('Fed', 'SS1', 'SS2', 'Medicare1', 'Medicare2', 'WALeave', 'WAUnemp', 'FedUnemp', 'EmployeeTax', 'EmployerTax'):
                t['{} {} YTD'.format(child, copy)] = ytd['{} {}'.format(child, copy)]

        for child in sconfig.children:
            ncalc = [(child, child+' ', ''), (child+' YTD', child+' ', ' YTD')]
            for dest, prefix, suffix in ncalc:
                n[dest] = (sums['sums'][prefix+'Gross'+suffix] - sums['tax'][prefix+'EmployeeTax'+suffix] + sums['sums'][prefix+'Reimbursements'+suffix]).quantize(CENTS, rounding=decimal.ROUND_UP)

    return ret
