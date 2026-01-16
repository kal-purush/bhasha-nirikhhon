import sys
import couchdb
import urllib, json

dbserver = 'https://taxigateway.cloudant.com/';
dbase = 'taxigateway'
couch_server = couchdb.Server(dbserver)
couch_database = couch_server[dbase]


def fetchFaults():
        url = dbserver + dbase + "/_design/list/_view/faults"
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        return data["rows"];

def checkChanges(rows):
        fault_count = 0;
        fault_total = 0;
        messages = "";
        for rowval in rows:
                row = rowval["value"]
                if not row.has_key("fixed_ts"): row["fixed_ts"] = 1
                if row["server_ts"] > row["fixed_ts"]:
                        fault_count = fault_count + 1;
                        messages = messages + row["_id"] + "/";
                fault_total = fault_total + 1;
        return [fault_total, fault_count, messages]
app = checkChanges(fetchFaults())
#http://nagios.sourceforge.net/docs/3_0/pluginapi.html
#http://www.shinken-monitoring.org/wiki/official/development-pluginapi
optdata = "\n|\n/=" + str(app[1]) + "of" + str(app[0]) + ";/" + app[2]
if app[1]:
        print ("Faults detected " + str(app[1]) + optdata)
        sys.exit(2)
else:
        print ("No pending apps" + optdata)
        sys.exit(0)