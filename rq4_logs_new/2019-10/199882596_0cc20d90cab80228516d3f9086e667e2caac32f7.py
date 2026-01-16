#************************************************************************
import os
import datetime as dt
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import cartopy.crs as ccrs

# internal utils
import qc_utils as utils
import io_utils as io
import qc_tests
import setup
#************************************************************************

QC_TESTS = {"o" : "Odd Cluster", "F" : "Frequent Value", "D" : "Distribution - Monthly", \
            "d" : "Distribution - all", "W" : "World Records", "K" : "Streaks", \
            "C" : "Climatological", "T" : "Timestamp", "S" : "Spike", "h" : "Humidity", \
            "V" : "Variance", "p" : "Pressure", "w" : "Winds"}

# Temporary stuff
MFF_LOC = setup.SUBDAILY_IN_DIR
QFF_LOC = setup.SUBDAILY_OUT_DIR
CONF_LOC = setup.SUBDAILY_CONFIG_DIR

IMAGE_LOCS=""
start_time_string = dt.datetime.strftime(dt.datetime.now(), "%Y%m%d")

#************************************************************************
def main(restart_id="", end_id="", diagnostics=False):
    """
    Main plot function.

    :param str restart_id: which station to start on
    :param str end_id: which station to end on
    :param bool diagnostics: print extra material to screen
    """

    obs_var_list = setup.obs_var_list

    # process the station list
    station_list = utils.get_station_list(restart_id=restart_id, end_id=end_id)

    station_IDs = station_list.iloc[:, 0]

    all_stations = {}

    # now spin through each ID in the curtailed list
    for st, station_id in enumerate(station_IDs):
        print("{} {}".format(dt.datetime.now(), station_id))

        station = utils.Station(station_id, station_list.iloc[st][1], station_list.iloc[st][2], station_list.iloc[st][3])
        if diagnostics:
            print(station)

        #*************************
        # read QFF
        station_df = io.read(os.path.join(QFF_LOC, station_id), extension="qff")

        for var in obs_var_list:

            setattr(station, var, utils.Meteorological_Variable("{}".format(var), utils.MDI, "", ""))
            obs_var = getattr(station, var)

            flags = station_df["{}_QC_flag".format(var)].fillna("")

            for test in QC_TESTS.keys():

                locs = flags[flags.str.match(test)]

                setattr(obs_var, test, locs.shape[0]/flags.shape[0])
            
        all_stations[station_id] = station

    # now spin through each var/test combo and make a plot
    for var in obs_var_list:
        for test in QC_TESTS.keys():

            lats, lons, flag_fraction = np.zeros(station_IDs.shape[0]), np.zeros(station_IDs.shape[0]), np.zeros(station_IDs.shape[0])
            for st, (ID, station) in enumerate(all_stations.items()):
                lats[st] = station.lat
                lons[st] = station.lon
                obs_var = getattr(station, var)
                flag_fraction[st] = getattr(obs_var, test) * 100               

            # do the plot
            plt.figure(figsize=(8, 5))
            plt.clf()
            ax = plt.axes([0, 0.03, 1, 1], projection=ccrs.Robinson())
            ax.set_global()
            ax.coastlines('50m')
            try:
                ax.gridlines(draw_labels = True)
            except TypeError:
                ax.gridlines()

            # colors are the exact same RBG codes as in IDL
            colors = [(150, 150, 150), (41, 10, 216), (63, 160, 255), (170, 247, 255), \
                      (255, 224, 153), (247, 109, 94), (165, 0, 33), (0, 0, 0)]
            limits = [0.0, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 100.]

            for u, upper in enumerate(limits):

                # sort the labels
                if u == 0:
                    locs, = np.where(flag_fraction == 0)
                    label = "{}%: {}".format(upper, len(locs))
                else:
                    locs, = np.where(np.logical_and(flag_fraction <= upper, \
                                                    flag_fraction > limits[u-1]))
                    label = ">{} to {}%: {}".format(limits[u-1], upper, len(locs))
                    if upper == limits[-1]:
                        label = ">{}%: {}".format(limits[u-1], len(locs))

                # and plot
                if len(locs) > 0:
                    ax.scatter(lons[locs], lats[locs], transform = ccrs.Geodetic(), s = 15, c = tuple([float(c)/255 for c in colors[u]]), \
                               edgecolors="none", label = label)

                else:
                    ax.scatter([0], [-90], transform = ccrs.Geodetic(), s = 15, c = tuple([float(c)/255 for c in colors[u]]), \
                               edgecolors="none", label = label)

            plt.title("{} - {}".format(" ".join([v.capitalize() for v in var.split("_")]), QC_TESTS[test]))

            watermarkstring="/".join(os.getcwd().split('/')[4:])+'/'+\
                os.path.basename( __file__ )+"   "+dt.datetime.strftime(dt.datetime.now(), "%d-%b-%Y %H:%M")
            plt.figtext(0.01,0.01,watermarkstring,size=5)

            leg=plt.legend(loc='lower center', ncol=4, bbox_to_anchor=(0.5, -0.12), frameon=False, title='', prop={'size':9}, \
                           labelspacing=0.15, columnspacing=0.5, numpoints=1)

            plt.savefig(IMAGE_LOCS+"All_fails_{}-{}_{}.png".format(var, test, start_time_string))
            plt.close()


#************************************************************************
if __name__ == "__main__":
    main()
#*******************************************************