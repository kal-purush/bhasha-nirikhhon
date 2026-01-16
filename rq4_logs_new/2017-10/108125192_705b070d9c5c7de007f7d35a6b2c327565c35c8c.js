// Creating variables for today's sunrise and sunset
  var sunrise = '2017-10-26 08:12:29';
  var sunset = '2017-10-26 17:52:06';

// Generating timestamps for the times
  var sunriseTimestamp = (moment('2017-10-26 08:12:29').unix())*1000;
  var sunsetTimestamp = (moment('2017-10-26 17:52:06').unix())*1000;

// Calculating the difference in seconds between today's sunrise and sunset
  var t1 = new Date(sunrise);
  var t2 = new Date(sunset);
  var dif = t1.getTime() - t2.getTime();

  var Seconds_from_T1_to_T2 = dif / 1000;
  var Seconds_Between_Dates = Math.abs(Seconds_from_T1_to_T2);


  var totalTime = '';
  var timeLeft = countdown(sunsetTimestamp, null, countdown.SECONDS);

  var percentage = 1 - (timeLeft/totalTime);

  var count = moment("1754", "hhmm").fromNow();
  document.write(count + '<br><br>');