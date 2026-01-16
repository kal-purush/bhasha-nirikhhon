import sys, os
if sys.platform == 'darwin':
    sys.path.append(os.path.join("/Users", "njchiang", "GitHub", "task-fmri-utils"))
    sys.path.append(os.path.join("Users", "njchiang", "GitHub", "python-fmri-utils", "utils"))
else:
    sys.path.append(os.path.join("D:\\", "GitHub", "task-fmri-utils"))
    sys.path.append(os.path.join("D:\\", "GitHub", "python-fmri-utils", "utils"))

zs = lambda v: (v - v.mean(0)) / v.std(0)  # z-score function
######################################
# initialize paths
PROJECTNAME = ""
TOOLBOXNAME = ""

def initpaths():
    print "Initializing..."
    import os
    import sys
    import pandas as pd
    p = []
    if 'win' in sys.platform:
        root = "D:\\"
        dataroot = os.path.join(root, "fmri")
    elif 'darwin' in sys.platform:
        root = os.path.join('/Users', 'njchiang')
        dataroot = os.path.join('/Volumes', 'JEFF', 'UCLA')

    p.append(os.path.join(dataroot, PROJECTNAME))
    p.append(os.path.join(root, 'GitHub', TOOLBOXNAME))
    p.append(os.path.join(root, 'CloudStation', 'Grad', 'Research', PROJECTNAME))
    c = pd.read_csv(os.path.join(p[1], 'labels', 'conds_key.txt'), sep='\t').to_dict('list')
    s = {}
    # dict of subjects and runs
    return p, s, c


######################################
# subject data I/O
def loadsubbetas(p, s, method="LSS", btype='tstat', m=None):
    # load subject data with paths list, s: sub, c: contrast, m: mask
    print s
    import os
    bsp = os.path.join(p[0], "data", s, "preproc")
    bsn = str(s + "_" + method + "_" + btype + ".nii.gz")
    bs = os.path.join(bsp, bsn)
    mnp = os.path.join(p[0], "data", s, "masks")
    mn = str(s + "_" + m + ".nii.gz")
    mf = os.path.join(mnp, mn)
    from mvpa2.datasets.mri import fmri_dataset
    fds = fmri_dataset(samples=bs, mask=mf)
    import pandas as pd
    lp = os.path.join(p[0], "data", s, "behav", "labels")
    attrs = pd.read_csv(os.path.join(lp, str(s + "_" + method + "_betas.tsv")), sep='\t')
    fds.sa['chunks'] = attrs['run'].tolist()
    for c in attrs.keys():
        fds.sa[c] = attrs[c].tolist()
    return fds


def preprocess_betas(paths, sub, btype="LSS", c="trial_type", roi="grayMatter", z=True):
    import projectutils as pu
    rds = pu.loadsubbetas(paths, sub, btype=btype, m=roi)
    rds.sa['targets'] = rds.sa[c]
    if z:
        zscore(rds, chunks_attr='chunks')
    return rds


def loadrundata(p, s, r, m=None, c='trial_type'):
    # inputs:
    # p: paths list
    # s: string representing subject ('LMVPA001')
    # r: run ID ('Run1')
    from os.path import join as pjoin
    from mvpa2.datasets import eventrelated as er
    from mvpa2.datasets.mri import fmri_dataset
    from mvpa2.datasets.sources import bids as bids
    # motion corrected and coregistered
    bfn = pjoin(p[0], 'data', s, 'preproc', s + '_' + r + '.nii.gz')
    if m is not None:
        m = pjoin(p[0], 'data', s, 'masks', s+'_'+m+'.nii.gz')
        d = fmri_dataset(bfn, chunks=int(r.split('n')[1]), mask=m)
    else:
        d = fmri_dataset(bfn, chunks=int(r.split('n')[1]))
    # This line-- should be different if we're doing GLM, etc.
    efn = pjoin(p[0], 'data', s, 'preproc', s + '_' + r + '.tsv')
    fe = bids.load_events(efn)
    if c is None:
        tmpe = events2dict(fe)
        c = tmpe.keys()
    if isinstance(c, basestring):
        # must be a list/tuple/array for the logic below
        c = [c]
    for ci in c:
        e = adjustevents(fe, ci)
        d = er.assign_conditionlabels(d, e, noinfolabel='rest', label_attr=ci)
    return d


def loadsubdata(p, s, m=None, c=None):
    from mvpa2.base import dataset
    fds = {}
    for sub in s.keys():
        print 'loading ' + sub
        rds = [loadrundata(p, sub, r, m, c) for r in s[sub]]
        fds[sub] = dataset.vstack(rds, a=0)
    return fds


def preprocess_data(paths, sublist, sub, filter_params=[49,2], roi="grayMatter", z=True):
    import projectutils as pu
    dsdict = pu.loadsubdata(paths, sublist[sub], m=roi)
    tds = dsdict[sub]
    beta_events = pu.loadevents(paths, sublist[sub])
    # savitsky golay filtering
    import SavGolFilter as SGF
    SGF.sg_filter(tds, filter_params[0], filter_params[1])
    # zscore entire set. if done chunk-wise, there is no double-dipping (since we leave a chunk out at a time).
    if z:
        from mvpa2.mappers.zscore import zscore
        zscore(tds, chunks_attr='chunks')
    rds, events = pu.amendtimings(tds, beta_events[sub])
    return rds, events


def loadevents(p, s):
    # if isinstance(c, basestring):
    #     # must be a list/tuple/array for the logic below
    #     c = [c]
    fds = {}
    from mvpa2.datasets.sources import bids
    from os.path import join as pjoin
    for sub in s.keys():
        fds[sub] = [bids.load_events(pjoin(p[0], 'data', sub, 'func', sub + '_' + r + '.tsv')) for r in s[sub]]
    return fds


def loadmotionparams(p, s):
    import numpy as np
    import os
    res = {}
    for sub in s.keys():
        mcs = [np.loadtxt(os.path.join(p[0], 'data', sub, 'preproc',
                                       sub + '_' + r + '_mc', sub + '_' + r + '_mc.par'))
               for r in s[sub]]
        res[sub] = np.vstack(mcs)
    return res


############################################
# amend events
def adjustevents(e, c='trial_type'):
    import numpy as np
    # rounding for now, ONLY because that works for this dataset. But should not round for probe
    ee = []
    for i, d in enumerate(e):
        if 'intensity' in d.keys():
            ee.append({'onset': d['onset'],
                       'duration': d['duration'],
                       'condition': d[c],
                       'intensity': int(d['intensity'])})
        else:
            ee.append({'onset': d['onset'],
                       'duration': d['duration'],
                       'condition': d[c],
                       'intensity': 1})
    return ee


def replacetargets(d, ckey, c='trial_type'):
    import numpy as np
    if c in ckey:
        d.sa[c] = [ckey[c][np.where(st == ckey['trial_type'])[0][0]] for st in d.sa['trial_type']]
        d.sa['targets'] = d.sa[c]
    else:
        print "not a valid contrasts, did not do anything."
    return d


def sortds(d, c='trial_type'):
    ds = d.copy()
    idx = [i[0] for i in sorted(enumerate(ds.sa[c].value), key=lambda x: x[1])]
    ds = d[idx, :]
    return ds


def amendtimings(ds, b, extras=None):
    # have this add all of the extra stuff
    from mvpa2.datasets import eventrelated as er
    import numpy as np
    TR = np.median(np.diff(ds.sa.time_coords))
    idx = 0
    # events are loading wrong...
    theseEvents = b
    events = []
    for i, te in enumerate(theseEvents):
        for ev in te:
            ev['chunks'] = ds.sa['chunks'].unique[i]
            ev['onset'] += idx
            # ev['targets'] = ev['condition']
            if 'intensity' in ev:
                ev['amplitude'] = ev['intensity']
            else:
                ev['amplitude'] = 1
            # add extra regressors
            if extras is not None:
                for k in extras.keys():
                    if ('random' in k):
                        ev[k] = extras[k][extras['trial_type'] == ev['trial_type']][0]
            if ev['duration'] is not '0':
                events.append(ev)
        if i < len(b)-1:
            ds.sa['time_coords'].value[ds.chunks == i+2] += np.max(ds.sa['time_coords'][ds.chunks == i+1]) + TR
            idx = np.min(ds.sa['time_coords'][ds.chunks == i+2])
    return ds, events


def events2dict(events):
    evvars = {}
    for k in events[0]:
        try:
            evvars[k] = [e[k] for e in events]
        except KeyError:
            raise ValueError("Each event property must be present for all "
                             "events (could not find '%s')" % k)
    return evvars


#####################################
# regression stuff
def condensematrix(dm, pd, names, key, hrf='canonical', op='mult'):
    # returns condition with probe removed
    import copy as cp
    import numpy as np
    delays = None
    if hrf == 'fir':
        delays = []
        for i in dm.names:
            if i == 'constant':
                delays.append('-1')
            else:
                delays.append(i.split('_')[i.split('_').index('delay') + 1])
        delays = np.array(delays, dtype=int)

    if op == 'stack':
        for i in dm.names:
            if (i != 'constant'):
                if (i.split('_')[i.split('_').index(key) + 1] != '0'):
                    pd.append(dm.matrix[:, dm.names.index(i)])
                    names.append(i.replace('glm_label_', ''))
    else:
        idx = []
        for i in dm.names:
            if i == 'constant':
                idx.append('0')
            else:
                idx.append(i.split('_')[i.split('_').index(key)+1])
        idx = np.array(idx, dtype=float)

        if delays is not None:
            for d in np.arange(np.max(delays)+1):
                outkey = key + '_delay_' + str(d)
                outidx = idx[delays == d]
                pd.append(np.dot(dm.matrix[:, delays==d], outidx))
                names.append(outkey)
        else:
            pd.append(np.dot(dm.matrix, idx))
            names.append(key)

# new make_designmat from scratch
def make_designmat(ds, eorig, time_attr='time_coords', condition_attr='targets', design_kwargs=None, regr_attrs=None):
    # make glm regressors for all attributes. so loop through condition_attr and add them all...
    import copy
    from nipy.modalities.fmri.design_matrix import make_dmtx
    import numpy as np
    # Decide/device condition attribute on which GLM will actually be done
    if isinstance(condition_attr, basestring):
        # must be a list/tuple/array for the logic below
        condition_attr = [condition_attr]

    e = copy.deepcopy(eorig)  # since we are modifying in place
    glm_condition_attrs=[]
    for i, con in enumerate(condition_attr):
        glm_condition_attr = 'regressors_' + str(con)
        glm_condition_attrs.append(glm_condition_attr)
        for ei in e:
            if glm_condition_attr in ei:
                raise ValueError("Event %s already has %s defined.  Should not "
                                 "happen.  Choose another name if defined it"
                                 % (ei, glm_condition_attr))
            ei[glm_condition_attr] = \
                'glm_label_' + str(con) + '_' + '+'.join(str(ei[c]) for c in [con])

    evvars = events2dict(e)
    add_paradigm_kwargs = {}
    if 'amplitude' in evvars:
        add_paradigm_kwargs['amplitude'] = evvars['amplitude']
    if design_kwargs is None:
        design_kwargs = {}
    if regr_attrs is not None:
        names = []
        regrs = []
        for attr in regr_attrs:
            regr = ds.sa[attr].value
            # add rudimentary dimension for easy hstacking later on
            if regr.ndim < 2:
                regr = regr[:, np.newaxis]
            if regr.shape[1] == 1:
                names.append(attr)
            else:
                #  add one per each column of the regressor
                for i in xrange(regr.shape[1]):
                    names.append("%s.%d" % (attr, i))
            regrs.append(regr)
        regrs = np.hstack(regrs)

        if 'add_regs' in design_kwargs:
            design_kwargs['add_regs'] = np.hstack((design_kwargs['add_regs'],
                                                   regrs))
        else:
            design_kwargs['add_regs'] = regrs
        if 'add_reg_names' in design_kwargs:
            design_kwargs['add_reg_names'].extend(names)
        else:
            design_kwargs['add_reg_names'] = names

    X = {}
    for ci, con in enumerate(condition_attr):
        # create paradigm
        if 'duration' in evvars:
            from nipy.modalities.fmri.experimental_paradigm import BlockParadigm
            # NiPy considers everything with a duration as a block paradigm
            paradigm = BlockParadigm(
                con_id=evvars[glm_condition_attrs[ci]],
                onset=evvars['onset'],
                duration=evvars['duration'],
                **add_paradigm_kwargs)
        else:
            from nipy.modalities.fmri.experimental_paradigm \
                import EventRelatedParadigm
            paradigm = EventRelatedParadigm(
                con_id=evvars[glm_condition_attrs[ci]],
                onset=evvars['onset'],
                **add_paradigm_kwargs)
        X[con] = make_dmtx(ds.sa[time_attr].value, paradigm=paradigm, **design_kwargs)
        for i, reg in enumerate(X[con].names):
            ds.sa[reg] = X[con].matrix[:, i]
        if con in ds.sa.keys():
            ds.sa.pop(con)

        for reg in ds.sa.keys():
            if str(con)+'0' in reg:
                ds.sa['glm_label_probe'] = ds.sa.pop(reg)

    # concatenate X... add chunk regressors...
    # if 'chunks' in ds.sa.keys():
    #     for i in ds.sa['chunks'].unique:
    #         ds.sa['glm_label_chunks' + str(i)] = np.array(ds.sa['chunks'].value == i, dtype=np.int)
    return X, ds


def make_parammat(dm, hrf='canonical', zscore=False):
    # remove anything with a 0 and include probe as a feature
    # assuming dm is a dict
    import numpy as np
    out = dm[dm.keys()[0]]
    pd = []
    names = []
    for key in dm.keys():
        if key == 'motion':
            names.append('motion_0')
            pd.append(np.dot(dm[key].matrix, np.array([1, 0, 0, 0, 0, 0, 0])))
            names.append('motion_1')
            pd.append(np.dot(dm[key].matrix, np.array([0, 1, 0, 0, 0, 0, 0])))
            names.append('motion_2')
            pd.append(np.dot(dm[key].matrix, np.array([0, 0, 1, 0, 0, 0, 0])))
            names.append('motion_3')
            pd.append(np.dot(dm[key].matrix, np.array([0, 0, 0, 1, 0, 0, 0])))
            names.append('motion_4')
            pd.append(np.dot(dm[key].matrix, np.array([0, 0, 0, 0, 1, 0, 0])))
            names.append('motion_5')
            pd.append(np.dot(dm[key].matrix, np.array([0, 0, 0, 0, 0, 1, 0])))
        # hardcode stim and verb
        elif key == 'stim': # keys in which you stack (make multiple regresors) instead of multiply
            condensematrix(dm[key], pd, names, key, hrf, op='stack')
        else:
            condensematrix(dm[key], pd, names, key, hrf, op='mult')
    # don't need constant because normalized data
    # pd.append(np.ones(np.shape(pd[-1])))
    # names.append('constant')
    if zscore:
        out.matrix = zs(np.array(pd).T)
    else:
        out.matrix = np.array(pd).T
    out.names = names
    return out
