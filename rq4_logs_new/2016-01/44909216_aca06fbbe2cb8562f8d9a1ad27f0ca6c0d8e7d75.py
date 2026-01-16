"""
Data resource for querying data
"""
from flask_restful import Resource
from sqlalchemy import or_, extract, func, Integer, desc
from datetime import datetime, timedelta
from sqlalchemy.sql.expression import cast
from sqlalchemy.sql import text
import uuid
import time

from meerkat_api.util import row_to_dict, rows_to_dicts, date_to_epi_week, get_children
from meerkat_api import db, app
from meerkat_abacus.model import Data, Locations, Alerts
from meerkat_api.resources.variables import Variables
from meerkat_api.resources.epi_week import EpiWeek
from meerkat_api.resources.locations import TotClinics
from meerkat_api.resources import alerts
from meerkat_api.resources.explore import QueryVariable
from meerkat_abacus.util import get_locations


class CdReport(Resource):
    """Class for communical disease report"""
    def get(self, location, end_date=None):
        """ generates data for the CD report for the year until the end date for the given location"""
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_date = datetime.now()
        start_date = datetime(end_date.year, 1, 1)
        ret = {}
        #meta data
        ret["meta"] = {"uuid": str(uuid.uuid4()),
                       "project_id": 1,
                       "generation_timestamp": datetime.now().isoformat(),
                       "schema_version": 0.1
        }
        end_date = end_date - timedelta(days=1)
        ew = EpiWeek()
        epi_week = ew.get(end_date.isoformat())["epi-week"]

        ret["data"] = {"epi_week_num": epi_week,
                       "epi_week_date": end_date.isoformat(),
                       "project_epoch": start_date.isoformat()
        }
        all_alerts = alerts.get_alerts({"location": location})
        data = {}
        weeks = [i for i in range(1, epi_week + 1, 1)]
        data_list = [0 for week in weeks]
        for a in all_alerts:
            if a[0].date <= end_date and a[0].date > start_date:
                reason = a[0].reason
                report_status = None
                if a[1]:
                    status = a[1].data["status"]
                    if status == "Confirmed":
                        report_status = "confirmed"
                    elif status != "Disregarded":
                        report_status = "suspected"
                else:
                    report_status = "suspected"
                epi_week = ew.get(a[0].date.isoformat())["epi-week"]
                if report_status:
                    data.setdefault(reason, {"weeks": weeks,
                                             "suspected": list(data_list),
                                             "confirmed": list(data_list)})
                    data[reason][report_status][epi_week - 1] += 1
        ret["data"]["communicable_diseases"] = data
        return ret


class PublicHealth(Resource):
    """ Class to return data for the public health report """
    def get(self, location, start_date=None, end_date=None):
        """ generates date for the Jordan public health report for the year 
        up to epi_week for the given location"""
        start = time.time()
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            start_date = datetime.now().replace(month=1, day=1,
                                                         hour=0, second=0,
                                                         minute=0,
                                                         microsecond=0)

            if end_date:
                end_date = datetime.strptime(end_date,'%Y-%m-%d')
            else:
                end_date = datetime.now()
        ret={}
        #meta data
        ret["meta"] = {"uuid": str(uuid.uuid4()),
                       "project_id": 1,
                       "generation_timestamp": datetime.now().isoformat(),
                       "schema_version": 0.1
        }
        ew = EpiWeek()
        end_date = end_date - timedelta(days=1)
        epi_week = ew.get(end_date.isoformat())["epi-week"]
        ret["data"] = {"epi_week_num": epi_week,
                       "epi_week_date": end_date.isoformat(),
                       "project_epoch": datetime(2015,5,20).isoformat()
        }
        conn = db.engine.connect()
        location_name = db.session.query(Locations.name).filter(
            Locations.id == location).first().name

        if not location_name:
            return None
        ret["data"]["project_region"] = location_name
        #We first add all the summary level data
        tot_clinics = TotClinics()
        ret["data"]["clinic_num"] = tot_clinics.get(location)
        
        ret["data"]["global_clinic_num"] = tot_clinics.get(1)
        total_cases = get_variable_id("tot_1", start_date, end_date, location, conn)
        ret["data"]["total_cases"] = total_cases
        if total_cases == 0:
            total_cases = 1
        total_consultations = get_variable_id("reg_2", start_date, end_date, location, conn)
        ret["data"]["total_consultations"] = total_consultations
        
        female = get_variable_id("gen_2", start_date, end_date, location, conn)
        male = get_variable_id("gen_1", start_date, end_date, location, conn)
        ret["data"]["percent_cases_male"] = male / total_cases*100
        ret["data"]["percent_cases_female"] = female / total_cases*100
        less_5yo = get_variable_id("age_1", start_date, end_date, location, conn)
        ret["data"]["percent_cases_lt_5yo"] = less_5yo / total_cases*100

        if less_5yo == 0:
            less_5yo = 1
        presenting_complaint = get_variables_category("pc", start_date, end_date, location, conn)
        ret["data"]["percent_morbidity_communicable"] = presenting_complaint["Communicable disease"] / total_cases * 100
        ret["data"]["percent_morbidity_non_communicable"] = presenting_complaint["Non-communicable disease"] / total_cases * 100
        ret["data"]["percent_morbidity_mental_health"] = presenting_complaint["Mental Health"] / total_cases * 100

        #public health indicators
        ret["data"]["public_health_indicators"] = [
            make_dict("Cases Reported", total_cases, None)]
        modules = get_variables_category("public", start_date, end_date, location, conn)
        ret["data"]["public_health_indicators"].append(
            make_dict("Mental Health (mhGAP) algorithm followed",
                      modules["Mental Health (mhGAP)"],
                      modules["Mental Health (mhGAP)"] / total_cases * 100))
        ret["data"]["public_health_indicators"].append(
            make_dict("Child Health (IMCI) algorithm followed",
                      modules["Child Health (IMCI)"],
                      modules["Child Health (IMCI)"] / less_5yo * 100))
        ret["data"]["public_health_indicators"].append(
            make_dict("Reproductive Health screening",
                      modules["Reproductive Health"],
                      modules["Reproductive Health"] / total_cases * 100))
        ret["data"]["public_health_indicators"].append(
            make_dict("Laboratory results recorded",
                      modules["Laboratory Results"],
                      modules["Laboratory Results"] / total_cases * 100))
        ret["data"]["public_health_indicators"].append(
            make_dict("Prescribing practice recorded",
                      modules["Prescribing"],
                      modules["Prescribing"] / total_cases * 100))
        smoking_prevalence = get_variable_id("smo_1", start_date, end_date, location, conn)
        smoking_prevalence_ever = get_variable_id("smo_2", start_date, end_date, location, conn)
        smoking_non_prevalence_ever = get_variable_id("smo_3", start_date, end_date, location, conn)

        if (smoking_prevalence_ever + smoking_non_prevalence_ever) == 0:
            smoking_prevalence_ever = 1
            ret["data"]["public_health_indicators"].append(
                make_dict("Smoking prevalence (current)",
                          smoking_prevalence,
                          smoking_prevalence / (smoking_prevalence_ever+smoking_non_prevalence_ever) * 100))

        #Reporting sites
        locs = get_locations(db.session)
        sub_locs = get_children(location, locs)
        ret["data"]["reporting_sites"] = []
        for sl in sub_locs:
            num = get_variable_id("tot_1", start_date, end_date, sl, conn)
            ret["data"]["reporting_sites"].append(
                make_dict(locs[sl].name,
                          num,
                          num / total_cases * 100))


        #Alerts
        #        aggregate_alerts = AggregateAlerts
        alerts = db.session.query(
            Alerts.reason, func.count(Alerts.id).label("count")).filter(
                Alerts.date >= start_date,
                Alerts.date < end_date).group_by(Alerts.reason).order_by(desc("count")).limit(5)
        ret["data"]["alerts"]=[]
        for a in alerts.all():
            ret["data"]["alerts"].append(
                {"subject": a[0],
                 "quantity": a[1]})
        all_alerts = db.session.query(func.count(Alerts.id)).filter(
                Alerts.date >= start_date,
                Alerts.date < end_date)
        ret["data"]["alerts_total"] = all_alerts.first()[0]

        #Demographics
        ret["data"]["demographics"] = []
        age = get_variables_category("age_gender", start_date, end_date, location, conn)
        age_gender={}
        for a in age:
            gender,ac = a.split(" ")
            if ac in age_gender.keys():
                age_gender[ac][gender] = age[a]
            else:
                age_gender[ac] = {gender: age[a]}
        age_order=["<5", "5-9", "10-14", "15-19", "20-59", ">60"]
        for a in age_order:
            if a in age_gender.keys():
                a_sum = sum(age_gender[a].values())
            
                if a_sum == 0:
                    a_sum = 1
                ret["data"]["demographics"].append(
                    {"age": a,
                     "male": {"quantity": age_gender[a]["Male"],
                              "percent": age_gender[a]["Male"] / a_sum * 100
                     },
                     "female":{"quantity": age_gender[a]["Female"],
                               "percent": age_gender[a]["Female"]/float(a_sum)*100
                     }
                    })

        #Nationality
        nationality = get_variables_category("nationality", start_date, end_date, location, conn)
        tot_nat = sum(nationality.values())
        if tot_nat == 0:
            tot_nat=1
        ret["data"]["nationality"] = []
        for nat in sorted(nationality, key=nationality.get, reverse=True):
            if nationality[nat] > 0:
                ret["data"]["nationality"].append(
                    make_dict(nat,
                              nationality[nat],
                              nationality[nat] / tot_nat * 100))
        #Status
        status = get_variables_category("status", start_date, end_date, location, conn)
        tot_sta = sum(status.values())
        if tot_sta == 0:
            tot_sta = 1
        ret["data"]["patient_status"] = []
        for sta in sorted(status, key=status.get, reverse=True):
            if status[sta] > 0:
                ret["data"]["patient_status"].append(
                    make_dict(sta,
                              status[sta],
                              status[sta] / tot_sta * 100))


        #Presenting Complaint

        tot_pc = sum(presenting_complaint.values())
        if tot_pc == 0:
            tot_pc = 1
        ret["data"]["presenting_complaints"] = []
        for p in presenting_complaint:
            ret["data"]["presenting_complaints"].append(
                make_dict(p,
                          presenting_complaint[p],
                          presenting_complaint[p] / tot_pc * 100))

            
        ret["data"]["morbidity_communicable"] = get_disease_types("cd", start_date, end_date, location, conn)
        ret["data"]["morbidity_non_communicable"] = get_disease_types("ncd", start_date, end_date, location, conn)
        ret["data"]["mental_health"] = get_disease_types("mental", start_date, end_date, location, conn)

        ch={}
        query_variable = QueryVariable()
        child_disease = query_variable.get("age_1","for_child",
                                           end_date=end_date.strftime("%d/%m/%Y"),
                                           start_date=start_date.strftime("%d/%m/%Y"),
                                           only_loc=location)
        for chi in child_disease.keys():
            ch[chi] = child_disease[chi]["total"]

        tot_ch = sum(ch.values())
        if tot_ch == 0:
            tot_ch = 1
        ret["data"]["child_health"] = []
        for disease in top(ch):
            if ch[disease] > 0:
                ret["data"]["child_health"].append(
                    make_dict(disease,
                              ch[disease],
                              ch[disease] / tot_ch * 100))
        return ret




def get_disease_types(category, start_date, end_date, location, conn):
    """ Get and return top five disease of a type """
    diseases = get_variables_category(category, start_date,
                                      end_date, location, conn)
    tot_n = sum(diseases.values())
    if tot_n == 0:
        tot_n = 1

    ret = []
    for disease in top(diseases):
        if diseases[disease] > 0:
            ret.append(make_dict(disease,
                                 diseases[disease],
                                 diseases[disease] / tot_n * 100))
    return ret

def make_dict(name,quantity,percent):
    return {"title": name,
            "quantity": quantity,
            "percent": percent}

def top(values, number=5):
    return sorted(values,key=values.get,reverse=True)[:number]


qu = text("SELECT sum(CAST(data.variables ->> :variables_1 AS INTEGER)) AS sum_1  FROM data WHERE (data.variables ? :variables_2) AND data.date >= :date_1 AND data.date < :date_2 AND (data.country = :country_1 OR data.region = :region_1 OR data.district = :district_1 OR data.clinic = :clinic_1)")

def get_variable_id(variable_id, start_date, end_date, location, conn):
    """ Get the sum of variable_id between start and end date with location"""
    result = conn.execute(qu,variables_1 = variable_id, variables_2=variable_id,
                          date_1=start_date,date_2=end_date,country_1=location,
                          region_1=location,district_1=location,clinic_1=location).fetchone()[0]
    if result:
        return result
    else:
        return 0

variables_instance = Variables()
def get_variables_category(category, start_date, end_date, location, conn):
    """ Aggregate category between dates and with location"""

    variables = variables_instance.get(category)
    return_data = {}
    for variable in variables.keys():
        r = get_variable_id(variable,
                           start_date,
                           end_date,
                            location,
                            conn)
        return_data[variables[variable]["name"]] = r
    return return_data

# if __name__ == "__main__":
#     print(get_variables_category("cd", datetime(2016,1,1),datetime(2017,1,1)))