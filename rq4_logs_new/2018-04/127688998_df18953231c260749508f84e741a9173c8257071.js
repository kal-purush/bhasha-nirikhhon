const db_operations=require("./db");
/*
    insertsensor_reading(type,value),
	getLatestSensor_readings(type,num,callback),
	getSersorReadings_betweenTime(type,start,end,callback),
	getAvg_sensorReading_betweenTime(type,start,end,callback),
	close_db	
  */
db.insertsensor_reading(temperature,8.8);
db.getLatestSensor_readings(temperature,10,(err,results)=>{
	if(err) console.error(err);
	console.log(results);
});