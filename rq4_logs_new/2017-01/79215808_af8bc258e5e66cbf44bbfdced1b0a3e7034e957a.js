// JavaScript source code
var calculateTime = function(hour, angle) {
    var time;
		var totalHours = hour;
    var totalAngle = angle;
    var minutes = totalAngle / 6
    var minutesHand = 0;
    var hoursHand = 0;
    var maxHours = 12;
    var meridiem;
    if(totalHours > 0 && totalHours < 12){
    	meridiem = 'AM';
    } else{
    	meridiem = 'PM';
    }
    for(var i = 1; i <= totalHours; i++){
    	
    		if(hoursHand > maxHours){
        			//reset hour hand
      		  	hoursHand = 1;
              
        }
        hoursHand++;        
    }
    
    minutesHand = Math.ceil(minutes) + (hoursHand * 5);
    
    if(minutesHand == 60){
    	minutesHand = '00';
    } else if(minutesHand > 60){
    		minutesHand = minutesHand - 60;
    }
    if(minutesHand !== '00' && minutesHand > 0 && minutesHand < 10){
    	minutesHand = "0" + minutesHand;
    }
    
    return "Current Time is " + hoursHand + ":" + minutesHand + meridiem;  
    
}

calculateTime(11, 180);
alert(calculateTime());