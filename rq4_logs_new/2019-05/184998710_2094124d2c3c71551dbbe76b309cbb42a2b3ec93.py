    

##################################################3#########
## Climate App Assignment by Sanjay Mamidi -5/3/2019

## Modifed paths of all routes to be links for easier testing and usage. 
## Only works if Port number stays at 5000.
## Added thread option in create_engine to prevent thread expiry errors 
## Modified all returns from routes to be of type Dictonary for easier reading
###############################################################


import datetime as dt
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
# qp = sqlalchemy.pool.QueuePool(creator, pool_size=5, max_overflow=10, timeout=30, use_lifo=False)
# Set the check_same_thread = False to get over the thread expiration error. However should not be used 
# for writing only reading from the db.

engine = create_engine("sqlite:///resources/hawaii.sqlite",connect_args={'check_same_thread': False})

# reflect an existing database into a new model
Base = automap_base()
# reflect the 
Base.prepare(engine, reflect=True)

# We can view all of the classes that automap found
Base.classes.keys()

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB


#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"<a href=http://127.0.0.1:5000/api/v1.0/stations>Stations</a><br/>"
        f"<a href=http://127.0.0.1:5000/api/v1.0/precipitation>Precipitation</a><br/>"
        f"<a href=http://127.0.0.1:5000/api/v1.0/tobs>Tobs</a><br/>"
        f"<a href=http://127.0.0.1:5000/api/v1.0/StartDate/2017-11-20>StartDate</a><br/>"
        f"<a href=http://127.0.0.1:5000/api/v1.0/DatesofVacay/2017-11-20/2017-11-30>DatesofVacay</a>"
    )

   


@app.route("/api/v1.0/StartDate/<startdate>")
def StartDate(startdate):
    session = Session(engine)
    # Process date strings
    startdate = (startdate.split("-"))
    startdate = dt.date(int(startdate[0]) - 1, int(startdate[1]),int(startdate[2]))        
   
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= startdate).all()
    
    # # Create a dictionary from the row data and append to a list temps
    datetemp =[]
    for tmin, tavg,tmax in results:
        datetempd={}
        datetempd["Minimum Temp"] = tmin
        datetempd["Average Temp"] = tavg
        datetempd["Max Temp"] =tmax
        datetemp.append(datetempd)

    return jsonify(datetemp)

    



@app.route("/api/v1.0/DatesofVacay/<startdate>/<enddate>")
def DatesofVacay(startdate,enddate):
    session = Session(engine)
    # Process date start and end 
    startdate = (startdate.split("-"))
    enddate = (enddate.split("-"))
    startdate = dt.date(int(startdate[0]) - 1, int(startdate[1]),int(startdate[2]))        
    enddate = dt.date(int(enddate[0]) - 1, int(enddate[1]),int(enddate[2])) 
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= startdate).filter(Measurement.date <= enddate).all()
    
     # # Create a dictionary from the row data and append to a list temps
    datetemp =[]
    for tmin, tavg,tmax in results:
        datetempd={}
        datetempd["Minimum Temp"] = tmin
        datetempd["Average Temp"] = tavg
        datetempd["Max Temp"] =tmax
        datetemp.append(datetempd)

    return jsonify(datetemp)




@app.route("/api/v1.0/stations")
def stations():
    """Return a list of all Station IDs"""
    session = Session(engine)
    # Query for  distinct stations
    results = session.query(Measurement.station).\
    group_by(Measurement.station).all()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)



@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return  precipitation """
    session = Session(engine)
    # Query all 
   # Calculate the date 1 year ago from the last data point in the database
    maxdate= session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    maxdate = (maxdate[0].split("-"))
    lastyeardate = dt.date(int(maxdate[0]) - 1, int(maxdate[1]),int(maxdate[2]))        

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.prcp,Measurement.date).\
        filter(Measurement.prcp >= 0 ).\
        filter(Measurement.date >= lastyeardate).all()

    # Create a dictionary from the row data and append to a list of dates and precipitation
    dateprcp =[]
    for prcp, pdate in results:
        dateprcpdict={}
        dateprcpdict["date"] = pdate
        dateprcpdict["rain"] = prcp
        dateprcp.append(dateprcpdict)

    return jsonify(dateprcp)

   

@app.route("/api/v1.0/tobs")
def tobs():
    """Return  precipitation """
    session = Session(engine)
    # Query all 
   # Calculate the date 1 year ago from the last data point in the database
    maxdate= session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    maxdate = (maxdate[0].split("-"))
    lastyeardate = dt.date(int(maxdate[0]) - 1, int(maxdate[1]),int(maxdate[2]))        

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.tobs,Measurement.date).\
        filter(Measurement.prcp >= 0 ).\
        filter(Measurement.date >= lastyeardate).all()

    # Create a dictionary from the row data and append to a list of dates and temp
    datetobs =[]
    for tobs, pdate in results:
        datetobsdict={}
        datetobsdict["date"] = pdate
        datetobsdict["temp"] = tobs
        datetobs.append(datetobsdict)

    return jsonify(datetobs)



if __name__ == '__main__':
    app.run(debug=True)