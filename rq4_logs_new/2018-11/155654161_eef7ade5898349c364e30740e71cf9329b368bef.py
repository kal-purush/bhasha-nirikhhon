import numpy as np
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
engine = create_engine("sqlite:///Resources/hawaii.sqlite?check_same_thread=False")
Base = automap_base()
Base.prepare(engine, reflect=True)
Measurement = Base.classes.measurement
Stations = Base.classes.station
session = Session(engine)
last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
one_year_ago = dt.date(2017,8,23)-dt.timedelta(days=365)
app = Flask(__name__)
@app.route("/")
def welcome():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date<br/>"
        f"/api/v1.0/start_date/end_date<br/>"
    )
@app.route("/api/v1.0/precipitation")
def prcp():
    precip_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date>=one_year_ago).all()
    precip_scores = []
    for precip in precip_data:
        precip_dict = {}
        precip_dict["date"] = precip.date
        precip_dict["prcp"] = precip.prcp
        precip_scores.append(precip_dict)
    return jsonify(precip_scores)
@app.route("/api/v1.0/stations")
def station():
    stations_activity = session.query(Measurement.station).all()
    all_stations = list(np.ravel(stations_activity))
    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    temp_data = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date>=one_year_ago).filter(Measurement.station=="USC00519281").all()
    temp_list = []
    for temp in temp_data:
        temp_dict = {}
        temp_dict["date"] = temp.date
        temp_dict["tobs"] = temp.tobs
        temp_list.append(temp_dict)
    return jsonify(temp_list)
@app.route("/api/v1.0/<start_date>")
def start_date(start_date):
    results = session.query(func.avg(Measurement.tobs), func.min(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date>=start_date).all()
    all_results = list(np.ravel(results))
    return jsonify(all_results)
@app.route("/api/v1.0/<start_date>/<end_date>")
def date_range(start_date, end_date):
    results = session.query(func.avg(Measurement.tobs), func.min(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date.between(start_date, end_date)).all()
    all_results = list(np.ravel(results))
    return jsonify(all_results)
if __name__ == '__main__':
    app.run(debug=True)