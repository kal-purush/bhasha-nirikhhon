from models import *
from health_care_portal import HealthCarePortal
from flask import *


# PATH = os.getcwd()
# os.chdir(PATH)


app = Flask(__name__) 
SQLAlchemy().init_app(app)

hcp = HealthCarePortal()

@app.route("/")
def health_care_portal_home():

    description = hcp.description

    return render_template("health_care_portal_home.html", texts=description)

@app.route("/emp_search/last", methods=['get', 'post'])
def emp_search_lastname():

    if request.method == 'POST':
        input = str(request.form["last_name"]).strip(" ")
        return hcp.emp_search_by_last(input)
    else:
        return render_template('emp_search_last.html')

@app.route("/emp_search/salary_range", methods=['get', 'post'])
def salary_range():
    if request.method == 'POST':
        input_min, input_max = request.form["min"].strip(" "), request.form["max"].strip(" ")
        return hcp.emp_search_salary_range(input_min, input_max)
    else:
        return render_template('emp_search_salary_range.html')

@app.route("/employee/portal",  methods=['get', 'post'])
def employee_portal():
    if request.method == 'POST':
        input_id = str(request.form["empID"]).strip(" ")
        return hcp.emp_portal(input_id)
    else:
        return render_template('employee_portal.html')
    
@app.route("/hr/portal", methods=['get', 'post'])
def hr_portal():
    if request.method == "POST":
        input_info = str(request.form["empID"]).strip(" ")
        if input_info.startswith("EE"):
            return employee_portal()
        else:
            return hcp.hr_portal(input_info)
    else:
        return render_template("hr_portal_med_code.html")

@app.route("/hr/summary")
def hr_summary():
    res = hcp.hr_summary()

    return render_template("hr_summary.html", data=res)

@app.route("/hr/benefit_cost")
def hr_benefit_cost():
    res = hcp.hr_benefit_cost()

    return render_template("hr_benefit_cost.html", data=res)

@app.route("/medical_procedure_analysis_sql")
def med_proc_analysis():
    res = hcp.med_proc_analysis_sql()

    return render_template("med_proc_analysis_sql.html", data=res)

if __name__ == "__main__":

    app.run(host="localhost", debug=True, port=5000)