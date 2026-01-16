
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
from dateutil.parser import parse
from pyspark.sql.functions import year, dayofmonth, month
import calendar
from time import strftime
from datetime import datetime
from pyspark.sql.functions import udf, count
import pandas as pd
from collections import OrderedDict

#plotting importfrom bokeh.charts import Histogram
from bokeh.charts import Scatter, Histogram, Bar,defaults, vplot, hplot, show, output_file, show
from bokeh.charts.attributes import cat, color
from bokeh.charts.operations import blend
import numpy as np
import math



import matplotlib.pyplot as plt
from datetime import datetime
start_time = datetime.now()


defaults.width = 450
defaults.height = 350

sc = SparkContext()
sqlContext = SQLContext(sc)


data=sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/Users/jayanthsivakumar/Desktop/spark-2.1.0-bin-hadoop2.7/samplecrime.csv')
data = sqlContext.read.load('/Users/jayanthsivakumar/Desktop/spark-2.1.0-bin-hadoop2.7/samplecrime.csv',format='com.databricks.spark.csv', header='true', inferSchema='true')
#data.printSchema()
data = data.fillna(0)
data = data.rdd
parse_time = udf(lambda time:dt.strptime(time, '%m/%d/%Y %I:%M:%S %p'))

crime_data = data.map(lambda d:(int(d[0]),d[1],parse(d[2]),d[3],d[4],d[5],d[6],d[7],d[8],d[9],int(d[10]),int(d[11]),int(d[12]),
int(d[13]),d[14],int(d[15]),
int(d[16]),int(d[17]),parse(d[18]),float(d[19]),float(d[20]),d[21]))


crime_df = sqlContext.createDataFrame(crime_data,["ID","Case Number","Date","Block","IUCR","Primary Type","Description",
"Location Description","Arrest","Domestic","Beat","District","Ward","Community Area","FBI Code","X Coordinate",
"Y Coordinate","Year","Updated On","Latitude","Longitude","Location"])


#Reduced level dataset for analysis
crime_detail = crime_df.select(year(crime_df.Date).alias("Year"),month(crime_df.Date).alias("Month"),dayofmonth(crime_df.Date).alias("DoM"),date_format(crime_df.Date, 'EEEE').alias("DoW"),hour(crime_df.Date).alias("Hour"),crime_df.Block,crime_df["Primary Type"].alias("CrimeType"),crime_df.Description,crime_df["Location Description"].alias("LocDesc"), crime_df.Arrest,crime_df.Domestic,crime_df.Beat,crime_df.District,crime_df.Ward,crime_df["Community Area"].alias("CommunityArea"),crime_df.Latitude,crime_df.Longitude,crime_df.Location)

crime_detail.registerTempTable("CrimeDetails")

#Top level analysis for the chicago crimes dataset

#print crime_detail.printSchema()


print "Total Records:           %d" % (crime_detail.count())
print "Distinct Year:           %d" % (crime_detail.select('Year').distinct().count())
print "Distinct Hours:          %d" % (crime_detail.select('Hour').distinct().count())
print "Distinct Type of crimes: %d" % (crime_detail.select('CrimeType').distinct().count())
print "Distinct Desc:           %d" % (crime_detail.select('Description').distinct().count())
print "Distinct Blocks:         %d" % (crime_detail.select('Block').distinct().count())
print "Distinct Loc Desc:       %d" % (crime_detail.select('LocDesc').distinct().count())
print "Distinct Beat:           %d" % (crime_detail.select('Beat').distinct().count())

print "Distinct District:       %d" % (crime_detail.select('District').distinct().count())
print "Distinct Ward:           %d" % (crime_detail.select('Ward').distinct().count())
print "Distinct Community Area: %d" % (crime_detail.select('CommunityArea').distinct().count())


#---------------------------ARREST RATE ANALYSIS------------------------------------
#Arrest Rate for true and false based on the crime types
arrestRate = crime_detail.stat.crosstab("CrimeType","Arrest").orderBy(desc('true')).limit(10)

arrestRate = arrestRate.select((arrestRate["CrimeType_Arrest"]).alias("CrimeType"),(arrestRate["false"]+arrestRate["true"]).alias("TotCount"),(arrestRate["false"]).alias("FalseCount"),(arrestRate["true"]).alias("TrueCount")).toPandas()


#Bar plot for different arrest rates ---------NOT STACKED PLOT-----------------
ax = arrestRate.plot(kind='bar',title ="Arrest Rate for different crimes - Non Stacked", figsize=(10, 8),legend=True, fontsize=8)
ax.set_xticklabels(arrestRate.CrimeType)
ax.set_xlabel('Crime Type')
ax.set_ylabel('Arrest/Not Arrested Total')
plt.show()



#Bar plot for different arrest rates -------STACKED PLOT-------------
ax = arrestRate[['TrueCount','FalseCount']].plot(kind='bar',title ="Arrest Rate for different crimes - Stacked", figsize=(10, 8), stacked = True,legend=True, fontsize=8)
ax.set_xticklabels(arrestRate.CrimeType)
ax.set_xlabel('Crime Type')
ax.set_ylabel('Arrest/Not Arrested Total')
plt.show()


#pie plot for arrested or not arrested counts
arrestRate.plot(kind='pie',title ="Arrest Rate for different crimes - Pie Chart", subplots=True,labels=arrestRate['CrimeType'],figsize=(10, 8), autopct='%1.1f%%',legend=True, fontsize=8)
plt.axis('equal')
plt.tight_layout()
plt.show()



#domestic Rate for true and false based on the crime types
domesticRate = crime_detail.stat.crosstab("CrimeType","Domestic").orderBy(desc('true')).limit(10)
domesticRate = domesticRate.select((domesticRate["CrimeType_Domestic"]).alias("CrimeType"),(domesticRate["false"]+domesticRate["true"]).alias("TotCount"),(domesticRate["false"]).alias("FalseCount"),(domesticRate["true"]).alias("TrueCount")).toPandas()


#Bar plot for different domestic rates ---------NOT STACKED PLOT-----------------
ax = domesticRate.plot(kind='bar',title ="Domestic/Non Domestic Crimes rate - Non Stacked", figsize=(10, 8),legend=True, fontsize=8)
ax.set_xticklabels(domesticRate.CrimeType)
ax.set_xlabel('Crime Type')
ax.set_ylabel('Domestic/Not Domestic Total')
plt.show()



#Bar plot for different domestic rates -------STACKED PLOT-------------
ax = domesticRate[['TrueCount','FalseCount']].plot(kind='bar',title ="Domestic/Non Domestic Crimes rate - Stacked", figsize=(10, 8), stacked = True,legend=True, fontsize=8)
ax.set_xticklabels(domesticRate.CrimeType)
ax.set_xlabel('Crime Type')
ax.set_ylabel('Domestic/Not Domestic Total')
plt.show()


#pie plot for domestic or not domestic counts
domesticRate.plot(kind='pie',title ="Domestic/Non Domestic Crimes rate - Pie Chart", subplots=True,labels=domesticRate['CrimeType'],figsize=(10, 8), autopct='%1.1f%%',legend=True, fontsize=8)
plt.axis('equal')
plt.tight_layout()
plt.show()




#Generating count histogram for crime types and crime happened analysis

groupedyear = sqlContext.sql("SELECT Year, COUNT(Year) AS YearCount from CrimeDetails GROUP BY Year ORDER BY Year").toPandas()
ax = groupedyear.plot(y='YearCount',kind='bar',title ="Total crimes for each year",figsize=(10, 8),legend=True, fontsize=8)
ax.set_xticklabels(groupedyear.Year)
ax.set_xlabel('Year')
ax.set_ylabel('Number of crimes in a Year')
plt.show()


groupedhour = sqlContext.sql("SELECT Hour, COUNT(Hour) AS HourCount from CrimeDetails GROUP BY Hour ORDER BY Hour").toPandas()
ax = groupedhour.plot(y='HourCount',kind='bar',title ="Total crimes for each Hour",figsize=(10, 8),legend=True, fontsize=8)
ax.set_xticklabels(groupedhour.Hour)
ax.set_xlabel('Hours of Day')
ax.set_ylabel('Number of crimes in each Hour')
plt.show()


groupeddom = sqlContext.sql("SELECT DoM, COUNT(DoM) AS DoMCount from CrimeDetails GROUP BY DoM ORDER BY DoM").toPandas()
ax = groupeddom.plot(y='DoMCount',kind='bar',title ="Total crimes for each day of month",figsize=(10, 8),legend=True, fontsize=8)
ax.set_xticklabels(groupeddom.DoM)
ax.set_xlabel('Day of the Month')
ax.set_ylabel("Number of crimes in a Month's day")
plt.show()

groupeddow = sqlContext.sql("SELECT DoW, COUNT(DoW) AS DoWCount from CrimeDetails GROUP BY DoW ORDER BY DoW").toPandas()
ax = groupeddow.plot(y='DoWCount',kind='bar', title ="Total crimes for each day of week",figsize=(10, 8),legend=True, fontsize=8)
ax.set_xticklabels(groupeddow.DoW)
ax.set_xlabel('Weekday')
ax.set_ylabel("Number of crimes in a Weekday")
plt.show()




groupedtypes = sqlContext.sql("SELECT CrimeType, COUNT(CrimeType) AS TypeCount from CrimeDetails GROUP BY CrimeType ORDER BY TypeCount DESC").toPandas()
ax = groupedtypes.plot(y='TypeCount',kind='bar',title ="Total crimes of each crime type",figsize=(10, 8),legend=True, fontsize=8)
ax.set_xticklabels(groupedtypes.CrimeType)
ax.set_xlabel('Crime Types')
ax.set_ylabel("Number of each Crime Types")
plt.show()



groupedmonth = sqlContext.sql("SELECT Month, COUNT(Month) AS MonthCount from CrimeDetails GROUP BY Month ORDER BY Month").toPandas()
ax = groupedmonth.plot(y='MonthCount',kind='bar',title ="Total crimes for each month",figsize=(10, 8),legend=True, fontsize=8)
ax.set_xticklabels(groupedmonth.Month)
ax.set_xlabel('Month')
ax.set_ylabel("Number of Crimes each Month")
plt.show()




end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))