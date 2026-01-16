function NextDate()  {
    var startdate = new Date(document.getElementById("lastdate").value);//get start date from user as a object
    var daystonext = Number(document.getElementById("eventinterval").value);  //number of days to next event
    
    var calcdate;
    calcdate = startdate.toDateString(startdate.setDate(startdate.getDate() + daystonext)); //next date calculation
    document.getElementById("nextevent").innerHTML = calcdate;
}

function CalculateNumbers() {
  var numberstocalc = Number(document.getElementById("desirednumbers").value); //collect the number of random numbers to calculate
  var lottonumber = 0; //store lottery number
  
  var count=0;
  var result = "<table><tr><th>Lottery Number </th><th>Results</th></tr>"; //create table headers
  for (count=0; count < numberstocalc; count++)  {
    lottonumber = Math.floor(Math.random() * 10000000 +1); //generate a 6 digit random integer
    result+= "<tr><td>" + (count + 1) + "</td><td>" + lottonumber + "</td></tr>";  //create table rows 
    
    
  }
    result += "</table>";
    document.getElementById("numberdisplay").innerHTML = result;  
      
  
} 