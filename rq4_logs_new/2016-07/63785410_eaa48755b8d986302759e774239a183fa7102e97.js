function GetInfo(){
	
	var battery = navigator.battery || navigator.webkitBattery || navigator.mozBattery;
	$("#BatteryInformation").empty();
   
    	

    	$("#WifiInformation").append("charging: " + (battery.charging ? 'charging' : 'not charging'));
      // Charging status: true or false 
      //document.querySelector('#charging').textContent = battery.charging ? 'charging' : 'not charging';

    	
    	$("#WifiInformation").append("charging Time: " + (battery.chargingTime / 60));
      // Battery charging time: seconds (for example, 3600) or Infinity 
      //document.querySelector('#chargingTime').textContent = battery.chargingTime / 60;

      // Battery life: seconds (for example, 3600) or Infinity 
      //document.querySelector('#dischargingTime').textContent = battery.dischargingTime / 60;

      // Battery.level: between 0 and 1 (for example, 0.50) 
      $("#WifiInformation").append("level: " + Math.floor(battery.level * 100) + '%');
      //document.querySelector('#level').textContent = Math.floor(battery.level * 100) + '%';   
   
   
}


function StartAlarm(){
	var alarm = new tizen.AlarmRelative(tizen.alarm.PERIOD_MINUTE/10);
	tizen.alarm.add(alarm, "org.tizen.browser");
}