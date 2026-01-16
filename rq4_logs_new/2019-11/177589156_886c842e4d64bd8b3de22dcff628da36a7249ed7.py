import os
import pandas as pd
import urllib
import numpy as np


# stores [observation source, source ID, Plotname]
GLCDICT = {
    'RGI60-11.00106': ['leclercq', 23, 'Pasterze (Austria)'],
    'RGI60-11.00746': ['leclercq', 62, 'Gepatschferner (Austria)'],
    'RGI60-11.00887': ['leclercq', 73, 'Gurgler'],
    'RGI60-11.00897': ['leclercq', 79, 'Hintereisferner'],
    'RGI60-11.00929': ['leclercq', 99, 'Langtaler'],
    'RGI60-11.00992': ['leclercq', 118, 'Nierderjoch'],

    'RGI60-11.01238': ['glamos', 'B43/03', 'Rhonegletscher'],
    'RGI60-11.01270': ['glamos', 'A54l/04', 'Upper Grindelwald glacier'],
    'RGI60-11.01328': ['glamos', 'A54g/11', 'Unteraargletscher'],
    'RGI60-11.01346': ['glamos', 'A54l/19',
                       'Lower Grindelwald glacier (Switzerland)'],
    'RGI60-11.01450': ['glamos', 'B36/26', 'Great Aletsch glacier'],
    'RGI60-11.01478': ['glamos', 'B40/07', 'Fiescher'],
    'RGI60-11.01698': ['glamos', 'B31/04', 'Langgletscher'],
    'RGI60-11.01946': ['glamos', 'E22/03', 'Vadret da Morteratsch'],

    'RGI60-11.01974': ['leclercq', 51, 'Forni (IT)'],

    'RGI60-11.02051': ['glamos', 'E23/06',
                       'Vadret da Tschierva (with Roseg) (Switzerland)'],
    'RGI60-11.02709': ['glamos', 'B72/15',
                       'Glacier du Mont Mine (with Ferpecle) (Switzerland)'],
    'RGI60-11.02245': ['glamos', 'C83/12', 'Forno'],
    'RGI60-11.02630': ['glamos', 'B63/05', 'Glacier de Zinal'],
    'RGI60-11.02704': ['glamos', 'B52/29', 'Allalingletscher'],
    'RGI60-11.02740': ['glamos', 'B90/02', 'Glacier du Trient'],
    'RGI60-11.02755': ['glamos', 'B73/16', 'Glacier de Tsijiore Nouve'],
    'RGI60-11.02793': ['glamos', 'B85/16', 'Glacier de Saleinaz'],
    'RGI60-11.02822': ['glamos', 'B56/07', 'Gornergletscher'],

    'RGI60-11.02916': ['leclercq', 432, 'Pre de Bard (IT)'],

    'RGI60-11.03638': ['leclercq', 7, 'Argentiere glacier (France)'],
    'RGI60-11.03643': ['leclercq', 109,
                       'Mer de Glace (with Leschaux) (France)'],
    'RGI60-11.03646': ['leclercq', 23, 'Bossons glacier (France)'],
}


def get_glamos(rgiids):

    glamos = pd.read_csv(os.path.join(os.path.dirname(__file__),
                                      'lengthchange.csv'),
                         header=6)

    dfmeta = pd.DataFrame([], index=rgiids, columns=['name', 'first',
                                                     'measurements', 'dL2003'])
    dfdata = pd.DataFrame([], index=rgiids, columns=np.arange(1850, 2020))

    for rgi in rgiids:
        if GLCDICT.get(rgi)[0] != 'glamos':
            # remove from meta and out and continue
            dfdata.drop(rgi)
            dfmeta.drop(rgi)
            continue

        # GLAMOS ID
        gid = GLCDICT.get(rgi)[1]

        # select glacier from glamos data
        glc = glamos.loc[glamos['glacier id'] == gid, :]

        # TODO make sure end-start in GLAMOS is 1 year!!!
        # find first year of observation
        t0 = pd.DatetimeIndex(glc['start date of observation']).year[0]
        dl = glc['length change'].astype(float).cumsum()
        yr = pd.DatetimeIndex(glc['end date of observation']).year

        # TODO make sure first year !>= 1850
        # TODO return some meta data

        # new dataframe for complete time series
        df = pd.Series(index=np.arange(1850, 2020))
        df.loc[yr] = dl.values
        df.loc[t0] = 0

        # get first measurement
        dfmeta.loc[rgi, 'first'] = t0

        # write data
        dfdata.loc[rgi] = df

        # special cases
        # Unterer Grindelwald
        if gid == 'A54l/19':

            # Bauder Email 02.10.2019
            dfdata.loc[rgi, 2007] = dfdata.loc[rgi, 1983] - 230
            dfdata.loc[rgi, 2008:] -= 230

        # meta stuff
        dfmeta.loc[rgi, 'name'] = GLCDICT.get(rgi)[2]
        dfmeta.loc[rgi, 'measurements'] = len(dfdata.loc[rgi].dropna())

        # get 2003 length change
        dfmeta.loc[rgi, 'dL2003'] = _get_2003_dl(dfdata.loc[rgi],
                                                 dfmeta.loc[rgi, 'name'])

    """
            # lets linearely interpolate a 2003 value for U. Grindelwald...
            dfout.loc[nr, 2003] = dfout.loc[nr].interpolate().loc[2003]
            glc.loc[2003] = dfout.loc[nr].interpolate().loc[2003]
    """
    return dfmeta, dfdata


def get_leclercq(rgiids):

    # --- Download and read Leclercq Files ---
    leclercqhttp = 'https://folk.uio.no/paulwl/downloads/'
    infofile = 'ALL_glacierinfo.txt'
    recordsfile = 'ALL_records.txt'
    urllib.request.urlretrieve(leclercqhttp + infofile, infofile)
    urllib.request.urlretrieve(leclercqhttp + recordsfile, recordsfile)
    lq_info = pd.read_csv(infofile, delimiter='\t')
    lq_rec = pd.read_csv(recordsfile, delimiter='\t', na_values='-99999')

    out = []

    y1850 = np.arange(1850, 2020)

    # loop over all Leclercq glaciers:
    lq_names = lq_rec.columns[0:-1:3]
    for glc in lq_names:

        # ---- 1. get Leclercq length record data
        # column in length record data
        icol = lq_rec.columns.get_loc(glc)
        # actual data
        year = np.array(lq_rec.iloc[1:, icol].dropna(), dtype=int)
        dL = np.array(lq_rec.iloc[1:, icol+1].dropna(), dtype=float)

        # ---- 2. get Leclercq glacier info
        # name and record number
        num = int(glc.split()[0])
        recname = glc.split()[1:]
        current_glc = lq_info.loc[lq_info['num '] == num]
        infoname = current_glc['name '].values[0]
        regio = current_glc['region '].values[0]

        # At the moment: only do Leclercq region 6
        if regio != 6:
            continue

        LID = current_glc['ID '].values[0]

        # check for consistency
        if 1:
            if not all(x in infoname for x in recname):
                raise ValueError('Name Error while processing %s' % infoname)
            elif not current_glc['first '].values[0] == year[0]:
                raise ValueError('First Year Error while processing %s' % infoname)
            elif not current_glc['last '].values[0] == year[-1]:
                raise ValueError('Last Year Error while processing %s' % infoname)

        # Save general information
        tmp = pd.DataFrame([], index=[LID], columns=y1850, dtype=float)
        tmp[year[year >= 1850]] = dL[year >= 1850]
        out.append(tmp)

    data = pd.concat(out)

    dfmeta = pd.DataFrame([], index=rgiids, columns=['name', 'first',
                                                     'measurements', 'dL2003'])
    dfdata = pd.DataFrame([], index=rgiids, columns=y1850)

    for rgi in rgiids:
        if GLCDICT.get(rgi)[0] != 'leclercq':
            # remove from meta and out and continue
            dfdata.drop(rgi)
            dfmeta.drop(rgi)
            continue

        # Leclercq ID
        lid = GLCDICT.get(rgi)[1]

        # get first measurement
        dfmeta.loc[rgi, 'first'] = data.loc[lid].dropna().index[0]

        # write data, and make it relative
        dfdata.loc[rgi] = data.loc[lid]
        dfdata.loc[rgi] -= dfdata.loc[rgi, 'first']

        # meta stuff
        dfmeta.loc[rgi, 'name'] = GLCDICT.get(rgi)[2]
        dfmeta.loc[rgi, 'measurements'] = len(dfdata.loc[rgi].dropna())

        # get 2003 length change
        dfmeta.loc[rgi, 'dL2003'] = _get_2003_dl(dfdata.loc[rgi],
                                                 dfmeta.loc[rgi, 'name'])

    return dfmeta, dfdata


def _get_2003_dl(glc, name=None):

    if np.isnan(glc.loc[2003]):
        try:
            minarg = np.abs(glc.loc[2000:2007].dropna().index-2007).argmin()
            dl2003 = glc.loc[2000:2007].dropna().iloc[minarg]
        except ValueError:
            print('No measurement around 2003... %s' % name)
            dl2003 = np.nan
    else:
        dl2003 = glc.loc[2003]

    return dl2003



def add_merge_length(meta, data, ids):

    glamos = pd.read_csv(os.path.join(os.path.dirname(__file__),
                                      'lengthchange.csv'),
                         header=6)
    raise RuntimeError('todo')
    for rgi in ids:

        if rgi == 'RGI60-11.02051':
            name = 'Vadret da Tschierva'
            merge_id = 'RGI60-11.02119'  # Roseg
        elif rgi == 'RGI60-11.02709':
            name = 'Glacier du Mont Miné'
            merge_id = 'RGI60-11.02715'  # Ferpecle
        else:
            raise ValueError('no data implemented')

        # LID of the glacier with the longer observation record
        merge_lid = meta.loc[meta.RGI_ID == merge_id].index[0]

        glc = get_glamos(name)

        # find first year of short observation
        t0_short = glc.dropna().index[0]

        # new dataframe for complete time series from the other glacier
        tmp = data.loc[merge_lid].copy()

        # find closest point in longer time series
        try:
            _ = data.loc[merge_lid].dropna().loc[t0_short]
            t0_long = t0_short
        except KeyError:
            minarg = np.abs(data.loc[merge_lid, t0_short-5:t0_short+5].
                            dropna().index - t0_short).argmin()
            t0_long = data.loc[merge_lid, t0_short-5:t0_short+5].\
                dropna().index[minarg]
            print('name t0 = %d (instead of %d)' % (t0_long, t0_short))

        # reference lenght at splitting time
        dl_ref = tmp.loc[t0_long]

        # reset data from t0_short onwards
        tmp.loc[t0_short:] = np.nan

        # new length is ref + dl_short
        tmp.loc[t0_short] = dl_ref
        tmp.loc[glc.dropna().index] = dl_ref + glc.dropna().values

        assert (tmp.index.unique() == tmp.index).all()

        # find 2003 length
        try:
            dl2003 = tmp.dropna().loc[2003]
        except KeyError:
            minarg = np.abs(tmp.loc[2000:2006].dropna().index-2003).argmin()
            dl2003 = tmp.loc[2000:2006].dropna().iloc[minarg]

        meta.loc[-1*int(rgi.split('.')[-1])] = {'name': name,
                                                'lon': -99,
                                                'lat': -99,
                                                'first': tmp.dropna().index[0],
                                                'measurements': len(tmp),
                                                'dL2003': dl2003,
                                                'RGI_ID': rgi}

        data.loc[-1*int(rgi.split('.')[-1])] = tmp.values

    return meta, data


def get_length_observations(rgiids):
    lec_meta, lec_data = get_leclercq(rgiids)
    gla_meta, gla_data = get_glamos(rgiids)

    meta, data = select_my_glaciers(meta_all, data_all)

    meta, data = add_merge_length(meta, data,
                                  ['RGI60-11.02051', 'RGI60-11.02709'])

    meta['first'] = meta['first'].astype(int)
    data.columns = data.columns.astype(int)

    return meta, data