#!/usr/bin/env python

from Bio import SeqIO
import argparse
import random, re, os
import time
import itertools

__author__ = "Franck Lejzerowicz"
__copyright__ = "Copyright 2017, The Deep-Sea Microbiome Project"
__credits__ = ["Yoann Dufresne", "Jan Pawlowski"]
__license__ = "GPL V3"
__version__ = "1.0"
__maintainer__ = "Franck Lejzerowicz"
__email__ = "franck.lejzerowicz@unige.ch"

def setMultiplex():
    """This script needs as input the tagged primers in fasta format, the prefix names of the forward and reverse pair(s), the output file name and a number of primer combinations. It only performs for short-construct primers corresponding to the fusion of n-nt long tags with an amplification primer. All tagged variants of an amplication must share the same prefix name, before a separator (default: "-")."""
    parser=argparse.ArgumentParser()
    parser.add_argument('-i', nargs='?', required=True, help='Tagged primers fasta file')
    parser.add_argument('-o', nargs='?', required=True, help='Output file name')
    parser.add_argument('-f', nargs='*', required=True, help='Forward primer generic name')
    parser.add_argument('-r', nargs='*', required=True, help='Reverse generic name')
    parser.add_argument('-n', nargs='?', type=int, required=True, help='Number of combinations to design. Enter different numbers in the same order of the different tags files')
    parser.add_argument('-l', nargs=2, type=int, default=[8, 8], help='Lengths of the forward and reverse tags (default = [8, 8])')
    parser.add_argument('-m', nargs='?', type=int, default=0, help='Set a maximum primer usage frequency (automatically changed if too low to be possible) (default = 0)')
    parser.add_argument('-s', nargs='?', type=int, default=3, help='Step-spacing between subsequent primers (if case of sequencial cross-contamination of the primers and if alphanumeric primer names) (default = 3)')
    parser.add_argument('-rm', nargs='*', default=[], help='Single tagged primers not to include (either provide file(s) with single primer name(s) in line(s), or write it fully as space-separated arguents e.g. "V4F-4 F1-A")')
    parser.add_argument('-rmCombi', nargs='*', default=[], help='File(s) with the combinations to avoid (fully written, space separated)')
    parser.add_argument('-set', nargs='*', default=[], help='Restict the choice of multiplexing design(s) to a list of combinations (file(s) with forward and reverse primers names in 1st and 2nd columns, repaectively')
    parser.add_argument('--one', action='store_true', default=False, help='Get the first selection only (default: off)')
    parser.add_argument('--nolog', action='store_false', default=True, help='Write a log file with the tested combination sets (default: on)')
    parser.add_argument('--random', action='store_true', default=False, help='Make random selection of mulitplexing design (default: off)')
    parser.add_argument('--brute', action='store_true', default=False, help='Make selection using brute-force algorithm based on forward primers selection first and then reverse primers until criteria satisfied (default: off)')
    parser.add_argument('--verbose', action='store_true', default=False, help='Print ongoing processes and results to stdout (default: off)')
    parse=parser.parse_args()
    args=vars(parse)

    tagFile = args['i']
    outFileName = args['o']
    nCombis = args['n']
    forNames = args['f']
    revNames = args['r']
    tagLen = args['l']
    maxRep = args['m']
    step = args['s']
    rmArg = args['rm']
    setArg = args['set']
    rmCombiArg = args['rmCombi']
    one = args['one']
    log = args['nolog']
    randomArg = args['random']
    bruteArg = args['brute']
    verbose = args['verbose']

    global verboseDebug
    verboseDebug = 0

    toRemove = get_list_from_args(rmArg)
    setChoice = get_list_from_args(setArg)
    rmCombis = get_list_from_args(rmCombiArg)
    if verbose:
        if len(toRemove):
            print 'To avoid primers:'
            print '  \n'.join(toRemove)
        if len(rmCombis):
            print 'To avoid combinations:'
            print '  \n'.join(rmCombis)
    tags, primersF, primersR = get_tags_dict(tagFile, forNames, revNames, toRemove, verbose)
    allCombis = get_all_combis(setChoice, primersF, primersR, nCombis, rmCombis)
    if verbose:
        print 'Candidate combinations:'
        print '  \n'.join(allCombis)

    if nCombis > len(allCombis):
        print 'Too much requested samples for the number of possible combinations (%s)' % (nCombis, len(allCombis))
        print 'Exiting...'
        return -1
    else:
        if verbose:
            print 'Multiplexing design saturation = %s %s (%s samples for %s possible combinations)' % (round(((float(nCombis)/len(allCombis))*100), 1), '%', nCombis, len(allCombis))

    oOut = open(outFileName, 'w')

    # prepare log file for writing every trial of multiplexing design
    if log:
        logFileName = '%s_%s.log' % (outFileName, '-'.join(time.strftime("%Y,%m,%d,%H,%M").split(',')))
        o = open(logFileName, 'w')
        o.write('%s\n' % tagFile)
        o.write('Options:\n')
        if randomArg:
            o.write('-algorithm\trandom\n')
        elif bruteArg:
            o.write('-algorithm\tbrute-force\n')
        else:
            if setChoice or rmCombis or len(primersF)!=len(primersR):
                o.write('-algorithm\tbrute-force\n')
            else:
                o.write('-algorithm\texact\n')
        o.write('-forNames\t%s\n' % '\t'.join(forNames))
        o.write('-revNames\t%s\n' % '\t'.join(revNames))
        o.write('-tagLen\t%s\n' % '\t'.join(map(str, tagLen)))
        o.write('-outFile\t%s\n' % outFileName)
        o.write('-nCombis\t%s\n' % nCombis)
        o.write('-maxRep\t%s\n' % maxRep)
        o.write('\n')

    maxRep = check_maxRep(primersF, primersR, nCombis, maxRep, verbose)

    maxreps = []
    while 1:
        if randomArg:
            print 'Random algorithm...',
            combis, numRep = make_random_design(allCombis, nCombis)
            maxreps.append(numRep)
        elif bruteArg:
            c = 0
            print 'Brute-force algorithm...',
            numRep = maxRep+1
            while numRep > maxRep:
                if c % 1000 == 0 and step > 1:
                    step = step - 1
                combis, numRep, step = make_pfpr_design(randomArg, primersF, primersR, allCombis, nCombis, maxRep, step, numRep, verbose)
                maxreps.append(numRep)
                c += 1
        else:
            if setChoice or rmCombis or len(primersF)!=len(primersR):
                print 'Exact algorithm not yet functional if -set or -rmCombi options activated, or if not squared matrix.'
                userChoice = raw_input('Continue with *** brute-force *** algorithm?\n<ENTER> to continue search / <ANYTHING> to stop: ')
                if userChoice:
                    return 0
                else:
                    bruteArg = True
                    continue
            print 'Exact algorithm...',
            lsd = make_lsd(nCombis, len(primersF))
            lsd_shuffle = shuffle_lsd(lsd)
            combis, numRep = translate_to_primers(lsd, primersF, primersR, allCombis)
            if combis==0:
                continue
            maxreps.append(numRep)

        primerSet, usage = get_primer_usage(combis, tags, tagLen)
        num = get_base_props(tagLen, primerSet)
        show_selection(num, usage, combis, primersF, primersR, verbose, log, o)
        # stop design search after first obtained
        if one:
            break
        # stop design search?
        userChoice = raw_input('\n<ENTER> to continue search / <ANYTHING> to stop: ')
        if userChoice:
            break
    num = get_base_props(tagLen, primerSet)
    show_last_selection(num, combis, verbose, log, o, oOut)

def make_lsd(n, size):
    lsd = [[0]*size for i in range(size)]
    nDiags = n / size
    remain = n % size
    divis = size / int(nDiags)
    starts = [0]
    for iStart in range(nDiags-1):
        curStart = starts[-1] + divis
        starts.append(curStart)
    for start in starts:
        curStart = start
        for rowdx, row in enumerate(lsd):
            curRow = rowdx
            if int(rowdx+start) == len(row):
                curStart = 0
            lsd[curRow][curStart] += 1
            curStart+=1
    if remain:
        largerGap = get_larger_gap(lsd[0], size)
        start = largerGap[len(largerGap)/2]
        curStart = start
        for rowdx, row in enumerate(lsd):
            if remain:
                curRow = rowdx
                if int(rowdx+start) == len(row):
                    curStart = 0
                lsd[curRow][curStart] += 1
                curStart+=1
                remain -= 1
    return lsd

def get_larger_gap(row, size):
    gaps = []
    gap = 0
    void = 0
    for idx, i in enumerate(row):
        if i:
            gaps.append([idx, void])
            void = 0
        else:
            void +=1
    if i:
        gaps.append([idx, void])
    else:
        gaps.append(['x', void])
    longest = [idx for idx, x in enumerate(gaps) if x[-1] == max([y[-1] for y in gaps])]
    longest = random.choice(longest)
    if longest == len(gaps)-1:
        return range(size-gaps[longest][-1], size)
    elif longest == 0:
        return range(longest[-1])
    else:
        return range(gaps[longest-1][0]+1, gaps[longest][0])


def shuffle_lsd(lsd):
    random.shuffle(lsd)
    return lsd

def translate_to_primers(lsd, primersF, primersR, allCombis):
    reps=[]
    combis = []
    colreps = [0]*len(lsd[0])
    for rdx, row in enumerate(lsd):
        reps.append(sum(row))
        for fdx, f in enumerate(row):
            if f:
                colreps[fdx]+=1
                curCombi = '%s %s' % (primersF[fdx], primersR[rdx])
                if curCombi not in allCombis:
                    return 0, 0
                combis.append(curCombi)
    for i in colreps:
        reps.append(i)
    return combis, max(reps)

def get_dPrimersF(pF, n, m):
    d = {}
    while sum(d.values()) != n:
        cur = random.choice(pF)
        if d.has_key(cur):
            if d[cur] >= m:
                continue
            d[cur] += 1
        else:
            d[cur] = 1
    return d

def get_revInit(fP, rPrimers, allCombis):
    rP = random.choice(rPrimers)
    curCombi = '%s %s' % (fP, rP)
    if verboseDebug:
        print 'fP, rPrimers'
        print fP, rPrimers
        print '> rP:', rP
        print 'curCombi'
        print curCombi
    while curCombi not in allCombis:
        rP = random.choice(rPrimers)
        curCombi = '%s %s' % (fP, rP)
    curRev = [rP]
    rPrimers.remove(rP)
    return rP, curRev

def get_candiD_dict(rPrimers, primersR, step, curRev):
    d= {}
    # for each remaining reverse primer
    for candi in rPrimers:
        dists = []
        okR = True
        # for each currently selected reverse primer
        for rev in curRev:
            # get the step distance
            curStep = abs(primersR.index(rev) - primersR.index(candi))
            if curStep < step:
                okR = False
            else:
                dists.append(curStep)
        if okR:
            # record in the dico the reverse primer that has his step value to the already selected reverse below the step value (with these steps as values)
            d[candi] = dists
    return d

def get_revNext(candiR_dico, fP, rPrimers, dPrimersR, allCombis, curRev, verbose):
    try:
        rP = random.choice(candiR_dico.keys())
        while '%s %s' % (fP, rP) not in allCombis:
            rP = random.choice(rPrimers)
        if verboseDebug:
            if dPrimersR.has_key(rP):
                print
                print '* 1 *', rP
                print dPrimersR[rP]
        curRev.append(rP)
    except IndexError:
        return 0, 0
    return rP, curRev

def make_pfpr_design(randomArg, primersF, primersR, allCombis, nCombis, maxRep, step, numRep, verbose):
    combis = []
    # get the forward primers usage frequencies dict {primer1: 3, primer2: 2, ...}
    dPrimersF = get_dPrimersF(primersF, nCombis, maxRep)
    # get a list with each reverse primer appearing maxRep times
    rPrimers = reduce(lambda n, m: n + m, [[x]*maxRep for x in primersR])
    if verboseDebug:
        print 'Current dPrimersF'
        print '-----------------'
        for v, w in enumerate(sorted(dPrimersF)):
            print 'idx, primer, number'
            print v, w, dPrimersF[w]
        print 'total:'
        print sum(dPrimersF.values())
        print rPrimers, len(rPrimers)
    dPrimersR = {}
    # for each forward primer present at least once
    for fP, maxP in sorted(dPrimersF.items()):
        # for each of the times it is present
        for indexP in range(0, maxP):
            if verboseDebug:
                print
                print 'Current forward primer', fP, '/', indexP
                print '---------------------------->'
            if indexP == 0:
                # get a random reverse primer and list initialized with this primer only
                rP, curRev = get_revInit(fP, rPrimers, allCombis)
                if verboseDebug:
                    print
                    print '*** 0 (1st choice):', rP
            else:
                # get a dict {}
                candiR_dico = get_candiD_dict(rPrimers, primersR, step, curRev)
                rP, curRev = get_revNext(candiR_dico, fP, rPrimers, dPrimersR, allCombis, curRev, verbose)
                if rP == 0:
                    break
                if verboseDebug:
                    print '*** 0 (2nd choice #1):', rP
                cc = 0
                step2 = 4
                while 1:
                    cc += 1
                    if cc % 1000 == 0 and step2 > 1:
                        step2 -= 1
                    elif cc == 5000:
                        break
                    if dPrimersR.has_key(rP):
                        curStep2 = abs(primersF.index(fP) - primersF.index(dPrimersR[rP]))
                        if curStep2 < step2:
                            rP = random.choice(rPrimers)
                            while '%s %s' % (fP, rP) not in allCombis:
                                rP = random.choice(rPrimers)
                            continue
                    rPrimers.remove(rP)
                    break
                if verboseDebug:
                    print '*** 0 (2nd choice #2):', rP
            combis.append('%s %s' % (fP, rP))
            dPrimersR[rP] = fP
        if len(combis) == nCombis:
            numRep = 0
            break
    return combis, numRep, step

def make_random_design(allCombis, nCombis):
    combis = random.sample(allCombis, nCombis)
    # get a list of numbers corresponding to the primers usage frequencies
    fPrimers = [[y.split(' ')[0] for y in combis].count(x) for x in list(set([z.split(' ')[0] for z in combis]))]
    # get a list of numbers corresponding to the primers usage frequencies
    rPrimers = [[y.split(' ')[1] for y in combis].count(x) for x in list(set([z.split(' ')[1] for z in combis]))]
    # get the maximum usage frequency for a primer
    numRep = max(fPrimers + rPrimers)
    return combis, numRep

def get_primer_usage(combis, tags, tagLen):
    primerSet = []
    usage = {0: {}, 1: {}}
    for i in combis:
        for n in [0,1]:
            p = i.split(' ')[n]
            #px = p.split('-')[-1]
            px = p
            primerSet.append(tags[p][:tagLen[n]])
            if usage[n].has_key(px):
                usage[n][px] += 1
            else:
                usage[n][px] = 1
    return primerSet, usage

def show_selection(num, usage, combis, primersF, primersR, verbose, log, o):
    if log:
        o.write('Candidate primer combinations:\n')
        for i in sorted(combis, key=lambda x: (x.split()[0], x.split()[-1])):
            o.write('%s\n' % i)
        o.write('\n')
        o.write('Percent of each base per position:\n')
        o.write('base\tA\tC\tG\tT\n')
        for i, j in sorted(num.items()):
            su=sum(j.values())
            o.write('%s\t%s\n' % (i, '\t'.join(map(str, [round((float(j[k])/float(su)*100), 2) for k in sorted(j)]))))
        o.write('\n')
        for j in primersF:
            for index, i in enumerate(primersR):
                if '%s%s' % (j, i) in ['%s%s' % (x.split()[0], x.split()[1]) for x in combis]:
                    o.write('%s+%s\t' % (j, i))
                else:
                    o.write('0\t')
                if index+1 == len(primersR):
                    if usage[0].has_key(j):
                        o.write('%s\t' % usage[0][j])
                    else:
                        o.write('0\t')
            o.write('\n')
        for i in primersR:
            if usage[1].has_key(i):
                o.write('%s\t' % usage[1][i])
            else:
                o.write('0\t')
        o.write('\n')
    if verbose:
        print
        print '=> Percent of each base per position:'
        print 'base\tA\tC\tG\tT'
        for i, j in sorted(num.items()):
            su=sum(j.values())
            print '%s\t%s' % (i, '\t'.join(map(str, [round((float(j[k])/float(su)*100), 2) for k in sorted(j)])))

        print
        for j in primersF:
            for index, i in enumerate(primersR):
                if '%s%s' % (j, i) in ['%s%s' % (x.split()[0], x.split()[1]) for x in combis]:
                    print '%s+%s\t' % (j, i),
                else:
                    print '0\t',
                if index+1 == len(primersR):
                    if usage[0].has_key(j):
                        print '%s\t' % usage[0][j],
                    else:
                        print '0\t',
            print
        for i in primersR:
            if usage[1].has_key(i):
                print '%s\t' % usage[1][i],
            else:
                print '0\t',

def show_last_selection(num, combis, verbose, log, o, oOut):
    if verbose:
        print 'percent of each base per position'
        print 'base\tA\tC\tG\tT'
        for i, j in sorted(num.items()):
            su=sum(j.values())
            print '%s\t%s' % (i, '\t'.join(map(str, [round((float(j[k])/float(su)*100), 2) for k in sorted(j)])))
    if len(combis):
        if verbose:
            print 'Selected primer combinations:'
        if log:
            o.write('\n')
            o.write('FINAL SELECTION\n')
            o.write('---------------\n')
        for i in sorted(combis, key=lambda x: (x.split()[0], x.split()[-1])):
            oOut.write('%s\n' % '\t'.join(i.split()))
            if verbose:
                print i
            if log:
                o.write('%s\n' % '\t'.join(i.split()))
        if log:
            o.close()
        oOut.close()

def get_base_props(tagLen, primerSet):
    num = {}
    for posN in range(0, max(tagLen)):
        curPos = {}
        for base in ['A','C','G','T']:
            curPos[base] = 0
        for tagSeq in primerSet:
            if len(tagSeq)>posN:
                curBase = tagSeq[posN]
                curPos[curBase] += 1
        num[posN+1] = curPos
    return num

def get_tags_dict(tagFile, forNames, revNames, toRemove, verbose):
    tags={}
    primersF=[]
    primersR=[]
    with open(tagFile) as f:
        for i in SeqIO.parse(f, 'fasta'):
            if str(i.id) in toRemove:
                continue
            tags[str(i.id)]=str(i.seq)
            if len(['x' for x in forNames if x in str(i.id)]) != 0:
                primersF.append(str(i.id))
            elif len(['x' for x in revNames if x in str(i.id)]) != 0:
                primersR.append(str(i.id))
    if verbose:
        print 'Forward tagged primers:'
        print '\n'.join(primersF)
        print 'Reverse tagged primers:'
        print '\n'.join(primersR)
    return tags, primersF, primersR

def get_list_from_args(args):
    l = []
    if args:
        for arg in args:
            if os.path.isfile(arg):
                with open(arg) as f:
                    for line in f:
                        l.append(' '.join(line.strip().split()))
            else:
                l.append(arg)
    return l

def get_all_combis(setChoice, primersF, primersR, nCombis, rmCombis):
    if setChoice:
        retSet = [x for x in setChoice if x not in rmCombis]
        return retSet
    else:
        retSet = []
        for combi in itertools.product(primersF, primersR):
            curCombi = ' '.join(combi)
            if curCombi not in rmCombis:
                retSet.append(curCombi)
        return retSet

def check_maxRep(primersF, primersR, nCombis, maxRep, verbose):
    n_pF = len(primersF)
    n_pR = len(primersR)
    forMax = maxRep * n_pF
    revMax = maxRep * n_pR
    if forMax < nCombis or revMax < nCombis:
        if verbose:
            print 'The requested max primer-usage frequency of %s is too low to design %s combinations' % (maxRep, nCombis)
        for n in range(maxRep+1, max([n_pF, n_pR])):
            if n * n_pF >= nCombis and n * n_pR >= nCombis:
                maxRep = n
                if verbose:
                    print 'Automatically set to %s' % maxRep
                break
    else:
        if verbose:
            print 'The requested max primer-usage frequency of %s is ok to design %s combinations' % (maxRep, nCombis)
    return maxRep

setMultiplex()