// Function StartTheCountdown is function that is called by the button Start Countdown
// The function will excute a build in function setInterval

  // Variable timeleft is the max number to start the countdown
  var timeleft = 10;
  //setInterval and clearInterval are built in functions that are use to start a loop until clear Interval is called.
  function startButton(){
   return downloadTimer = setInterval(setCounter,1000); // higher function setInterval that call setCounter for start the timer and 1000 sec.
  // function that start the counter
  function setCounter (){
    // if the time is less than the equal number of timeleft the clearInterval fucntion is called
    if (timeleft <= 0) {
      clearInterval(downloadTimer);
      // change the test of countdown innerHtml attribute string
      // document.getElementById("td1").innerHTML = "Blassoff"; // Add the countdown to the table // Remove is not needed
      document.getElementById("countdown").innerHTML = "Finished";
      //fontColor(red, td1); // call function to change color parameter 1 is the color parameter 2 is the id
      // When the if is true is called also the img id=megman will be called
      var img = document.getElementById("megaman");
      img.src = "rocket.gif";
    } else if (timeleft <= 5) {
      //Else for the special warning of if the countdown is less of 5 secs
      document.getElementById("countdown").innerHTML =
        "Warning Less than ½ way to launch, time left " + timeleft;
          // Table Number 
        // document.getElementById("td1").innerHTML = timeleft; // Add the countdown to the table
    } else {
      //here we have the function setInterval with concanate data for the timer.
      document.getElementById("countdown").innerHTML =
        timeleft + " seconds remaining";
        // Table Number
       //  document.getElementById("td1").innerHTML = timeleft; // Add the countdown to the table
    } // decreasing the the timeleft interger 1
    timeleft -= 1;
   }
  }
 

function login (){
  //Binding for data collection
  let uname = document.getElementById("uname").value; //first name data collection
  let lname = document.getElementById("lname").value; // last name data collection
  let pin = document.getElementById("pin").value // pin data collection
  const counter = (a, b) => {return a + b + 1;} // Function to count that count the character and +1 for the spacing between fname and lname
    
  // The If will check if the charater count OR if the counter is 1 character that is when the person dont fill the textbox
  if (20 < counter(uname.length, lname.length) || 1 == counter(uname.length, lname.length )) {

    alert("Please verified your name and last name not be longer of 20 character") // Alert for information for the user
    location.replace("Blassofflogin.html");  // Reset the page and clear the text box
    
    //Else if will check if the number has lower of 3 digit or if is 0 in case the person dont input data
  } else if (pin > 999 || pin == 0 ){
    alert("Please verified Pin Number") // Alert for information for the user
    location.replace("Blassofflogin.html"); // Reset the page and clear the text box
    
  } else {
    //Welcome pop up
    alert("Welcome UAT Space Program")
    location.replace("Blassoff copy.html"); // change of page
    
  }
  
}

// function to change the color of letters
function fontColor (newColor, htmlId) {
  let _countdown = document.getElementById("td1"); // binding the id and using the object
  _countdown.style.color = newColor; // selecte the color with the parameter

}
//Stop the timer
function stop(){
  //function stop setInterval timer.
  clearInterval(downloadTimer);
}

function back () {
  location.replace("../index.html"); // back button move you to the landing page
  }
