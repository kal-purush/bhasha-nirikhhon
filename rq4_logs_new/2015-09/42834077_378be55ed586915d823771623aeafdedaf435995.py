import csv
import json
import urllib2
import sys


if __name__=='__main__':
    url = (('http://api.prod.obanyc.com/api/siri/vehicle-monitoring.json?key=%s&VehicleMonitoringDetailLevel=calls&LineRef=%s')%(sys.argv[1],sys.argv[2]))
    request = urllib2.urlopen(url)
    data = json.loads(request.read())
    busInfo = (data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'])
    count_var = 0
    with open(sys.argv[3], 'w') as csvFile:  
        writer = csv.writer(csvFile)
        for b in busInfo:
            vehActivity = b['VehicleActivity']
            for i in vehActivity:
                count_var +=1
                vehicalLocation =i['MonitoredVehicleJourney']['VehicleLocation']
                stopInfo = i['MonitoredVehicleJourney']['OnwardCalls']['OnwardCall']
                vehicalName = vehActivity[0]['MonitoredVehicleJourney']['PublishedLineName']
                writer.writerow  ([('Bus Line : %s')%( str(vehicalName))])
                writer.writerow(['Bus Licence ',i['MonitoredVehicleJourney']['VehicleRef']])
                writer.writerow(['Bus  ',count_var])
                headers = ['Latitude', 'Longitude','Stop Name' ,'Stop Status' ]
                writer.writerow(headers)
                Latitude = vehicalLocation['Latitude']
                Longitude = vehicalLocation['Longitude']
                for stop in range(len(stopInfo)):
                    stopName=stopInfo[stop]['StopPointName']
                    if stopName == None:
                        stopName='N/A'
                    stopDistance=stopInfo[stop]['Extensions']['Distances']['PresentableDistance']
                    if stopDistance == None:
                        stopDistance = 'N/A'
                    writer.writerow([Latitude,Longitude,stopName,stopDistance])
                