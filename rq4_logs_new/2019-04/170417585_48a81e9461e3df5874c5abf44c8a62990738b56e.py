import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

import datetime as dt


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def index():
    """List all available api routes."""
    return (
        f"<b>Available Routes:</b><br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<<start>enter_start_date><br/>"
        f"/api/v1.0/<<start>enter_start_date>/<<end>enter_end_date>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return a list of all precipitation measures"""
    # Query all prcp values and dates
    results = session.query(Measurement.date, Measurement.prcp).all()
    
    # Create a dictionary from the row data and append to a list of all_prcp
    all_prcp = []
    for date, prcp in results:
        prcp_dict = {}
        prcp_dict["date"] = date
        prcp_dict["prcp"] = prcp
        all_prcp.append(prcp_dict)

    return jsonify(all_prcp)


@app.route("/api/v1.0/stations")
def stations():
    """Return a list of stations"""
    # Query all stations (distinct)
    results = session.query(Station.station).distinct().all()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)


@app.route("/api/v1.0/tobs")
def tobs():
    """Return a list of dates and temperatures from previous year"""
    
    # Calculate the date 1 year ago from the last data point in the database
    max_date = engine.execute('select date from Measurement ORDER BY date desc limit 1').fetchall()[0][0]
    
    # Subtract 12 months/1 year from date for timeframe
    entered_date = dt.datetime.strptime(max_date, '%Y-%m-%d')
    entered_date = entered_date.date()
    twelve_months_ago = entered_date - dt.timedelta(days=365)

    # Perform a query to retrieve the data and temperatures
    sel = [Measurement.date, 
           Measurement.tobs]
    results_tobs = session.query(*sel).\
        filter(func.strftime(Measurement.date) >= twelve_months_ago).all()
    
    # Convert list of tuples into normal list
    last_12_prcp = list(np.ravel(results_tobs))

    return jsonify(last_12_prcp)



@app.route("/api/v1.0/<start>/", defaults={"end": None})
@app.route("/api/v1.0/<start>/<end>")
def temp_summary_start_end(start, end):
    """Return a JSON list of the minimum temperature, the average temperature, and the max temperature
        for a given start date.  If end date is not entered, only return aggregates for all dates greater
        then the start date.  If both dates are entered, use these as a range of dates and return the
        aggregates for this range."""

    
    if end is None:
        
        date_filter = "m.date >= '" + start + "'"

        avg_start =  engine.execute("""SELECT   MIN(m.tobs),
                                                AVG(m.tobs),
                                                MAX(m.tobs)
                                       FROM     Measurement m
                                       WHERE    """ + date_filter
                                   ).fetchall()
    
        results = list(np.ravel(avg_start))

        return jsonify(results)
    
    else:
        date_filter = "m.date >= '" + start + "' AND m.date <= '" + end + "'"

        avg_start_end =  engine.execute("""SELECT   MIN(m.tobs),
                                                    AVG(m.tobs),
                                                    MAX(m.tobs)
                                           FROM     Measurement m
                                           WHERE    """ + date_filter
                                   ).fetchall()
    
        results = list(np.ravel(avg_start_end))

        return jsonify(results)
        



if __name__ == '__main__':
    app.run(debug=True)