import pandas as pd

def call_ssc_with_dataframe(df,lat=9.817934,lon=-84.070552,timezone=1,elevation=0):

    #import additional module for SAM simulation:
    import site
    # Use site.addsitedir() to set the path to the SAM SDK API. Set path to the python directory.
    site.addsitedir("/Applications/sam-sdk-2015-6-30-r3/languages/python/")
    import sscapi
    ssc = sscapi.PySSC()

    # Resource inputs for SAM model:
    wfd = ssc.data_create()
    ssc.data_set_number(wfd, "lat", lat)
    ssc.data_set_number(wfd, "lon", lon)
    ssc.data_set_number(wfd, "tz", timezone)
    ssc.data_set_number(wfd, "elev", elevation)
    ssc.data_set_array(wfd, "year", df.index.year)
    ssc.data_set_array(wfd, "month", df.index.month)
    ssc.data_set_array(wfd, "day", df.index.day)
    ssc.data_set_array(wfd, "hour", df.index.hour)
    ssc.data_set_array(wfd, "minute", df.index.minute)
    ssc.data_set_array(wfd, "dn", df["DNI"])
    ssc.data_set_array(wfd, "df", df["DHI"])
    ssc.data_set_array(wfd, "wspd", df["Wind Speed"])
    ssc.data_set_array(wfd, "tdry", df["Temperature"])

    # Create SAM compliant object  
    dat = ssc.data_create()
    ssc.data_set_table(dat, "solar_resource_data", wfd)
    ssc.data_free(wfd)

    # Specify the system Configuration
    # Set system capacity in MW
    system_capacity = 4
    ssc.data_set_number(dat, "system_capacity", system_capacity)
    # Set DC/AC ratio (or power ratio). See https://sam.nrel.gov/sites/default/files/content/virtual_conf_july_2013/07-sam-virtual-conference-2013-woodcock.pdf
    ssc.data_set_number(dat, "dc_ac_ratio", 1.1)
    # Set tilt of system in degrees
    ssc.data_set_number(dat, "tilt", 25)
    # Set azimuth angle (in degrees) from north (0 degrees)
    ssc.data_set_number(dat, "azimuth", 180)
    # Set the inverter efficency
    ssc.data_set_number(dat, "inv_eff", 96)
    # Set the system losses, in percent
    ssc.data_set_number(dat, "losses", 14.0757)
    # Specify fixed tilt system (0=Fixed, 1=Fixed Roof, 2=1 Axis Tracker, 3=Backtracted, 4=2 Axis Tracker)
    ssc.data_set_number(dat, "array_type", 0)
    # Set ground coverage ratio
    ssc.data_set_number(dat, "gcr", 0.4)
    # Set constant loss adjustment
    ssc.data_set_number(dat, "adjust:constant", 0)

    # execute and put generation results back into dataframe
    mod = ssc.module_create("pvwattsv5")
    ssc.module_exec(mod, dat)
    df["generation"] = np.array(ssc.data_get_array(dat, "gen"))

    # free the memory
    ssc.data_free(dat)
    ssc.module_free(mod)