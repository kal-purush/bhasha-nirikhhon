# to run this, at terminal, >import plot_optical

from astrodbkit import astrodb
import matplotlib.pyplot as plt
import numpy as np

# setup matplotlib to use latex
from matplotlib import rc
rc('font', **{'family': 'sans-serif', 'sans-serif': ['Helvetica']})
rc('text', usetex=True)

db = astrodb.Database('/Users/kelle/Dropbox/BDNYCdb/BDNYCdev.db')

# path to where spectra should be written to
out_path = '/Users/kelle/Dropbox/Analysis/NIRtemplates/NIRSpecFigures/plots/galaxies/'

# Explictly decide which spectra to plot
spectra_ids = [239,268,269,274,276,182,183,184,202,294,296,302,309,222,245,235,324]
ymaxs = [2.0,2.0,2.0,2.0,2.5,2.5,2.0,2.0,2.5,2.5,2.0,2.5,2.0,4,2.0,4,5 ]
# enter the strings by hand. these are not used.
spectral_type_strings = ["L0$\gamma$", "L2$\gamma$", "L1$\gamma$",
                        "L0$\gamma$", "L0$\gamma$", "L1$\gamma$",
                        "L2$\gamma$", "L5", "L4$\gamma$",
                        "L3.5$\gamma$", "L3.5$\gamma$", "L4$\gamma$",
                        "L4$\gamma$", "L0$\gamma$",
                        "L3.5$\gamma$",
                        "L1", "L5"]

ids_and_types = zip(spectra_ids, spectral_type_strings)

# look up names, obsdates, and spectral types for the spectra
names2 = []
obsdates = []
for spectra_id, spectral_type_string in ids_and_types:
    source_id2 = db.query("SELECT source_id from spectra where id={} ".format(spectra_id))[0][0]
    name2 = db.query("SELECT shortname from sources where id={} ".format(source_id2))[0][0]
    obsdate = db.query("SELECT obs_date from spectra where id={} ".format(spectra_id))[0][0]
    spectral_type = db.query(
        "SELECT spectral_type FROM spectral_types WHERE source_id={} AND adopted = 1".format(source_id2))
    sptype_gravity = db.query(
        "SELECT gravity FROM spectral_types WHERE source_id={} AND adopted = 1".format(source_id2))
    sptype_suffix = db.query(
        "SELECT suffix FROM spectral_types WHERE source_id={} AND adopted = 1".format(source_id2))
    names2.append(name2)
    obsdates.append(obsdate)
    print name2, source_id2, spectral_type, sptype_gravity, sptype_suffix, spectral_type_string, spectra_id, obsdate

for j, spectra_id in enumerate(spectra_ids):
        spectrum = db.query("SELECT spectrum from spectra where id='{}'".format(spectra_id), fetch='one')[0]
        wave = spectrum.data[0]
        flux = spectrum.data[1]

        plt.step(wave, flux / np.nanmean(flux), where='mid', linewidth=0.5, color = 'k' )
        #plt.show() #go to screen

        plt.xlim(0.8, 2.4)
        plt.ylim(0, ymaxs[j])
        plt.xlabel(r'Wavelength ($\mu$m)$')
        plt.ylabel('Normalized Flux')

        # change plot box to grey
        for spine in plt.gca().spines.values():
            spine.set_edgecolor('#BDBDBD')
        # change tick marks to grey
        plt.gca().tick_params(color='#BDBDBD')

        plt.annotate('{}'.format(names2[j]), xy=(0,0), xytext=(0.70, 0.8), xycoords='figure fraction')
        #plt.text(6500, 3.3, r'{}'.format(spectral_type_strings[j]))
        plt.annotate('obs date: ' '{}'.format(obsdates[j]), xy=(0,0), xytext=(0.70 , 0.75), size='smaller',xycoords='figure fraction')

        plt.savefig(out_path + '{}'.format(names2[j]) + '_{}'.format(spectra_id) + '.pdf')

        plt.close()