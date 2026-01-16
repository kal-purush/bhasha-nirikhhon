import boto3
import time
import csv
import smtplib 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders
#mention the profile name and aws region name
aws_mag_con = boto3.session.Session(profile_name ='xxx',region_name = "xxx")
ec2_cli = aws_mag_con.client("ec2")
x = "instance_state.csv"
with open(x,"w+") as f:
	con = csv.writer(f,delimiter = ",")
	con.writerow(["InstaneID","PrivateIP","Type","az","Launchtime"])
	for i in ec2_cli.describe_instances()["Reservations"]:
		if i["Instances"][0]["State"]["Name"] == "running":
			id = i["Instances"][0]["InstanceId"]
			ip = i["Instances"][0]["PrivateIpAddress"]
			type = i["Instances"][0]["InstanceType"]
			time = i["Instances"][0]['LaunchTime'].strftime("%Y-%m-%d")
			az = i["Instances"][0]["Placement"]["AvailabilityZone"]
			con.writerow([id,ip,type,az,time])
# include the from and to email addresses
fromaddr = "xxxx"
toaddr = "xxx"
msg = MIMEMultipart() 
msg['From'] = fromaddr 
msg['To'] = toaddr 
msg['Subject'] = "AWS || Running instances"  
body = ""
msg.attach(MIMEText(body, 'plain')) 
filename = "today_instance.csv"
attachment = open("/Users/pradeeshb/Desktop/today_instance.csv", "rb") 
p = MIMEBase('application', 'octet-stream') 
p.set_payload((attachment).read()) 
encoders.encode_base64(p) 
p.add_header('Content-Disposition', "attachment; filename= %s" % filename) 
msg.attach(p) 
# creates SMTP session 
s = smtplib.SMTP('smtp.gmail.com', 587) 
s.starttls() 
s.login(fromaddr, "Pradeesh@1993") 
text = msg.as_string() 
s.sendmail(fromaddr, toaddr, text) 
s.quit()