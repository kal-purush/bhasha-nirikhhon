function displayDateTime() {
    var now = new Date();
    var date = now.toDateString();
    var time = now.toLocaleTimeString();

    document.getElementById("date").innerHTML="Date: "+date;
    document.getElementById("time").innerHTML="Time: "+time;
}

setInterval(displayDateTime, 1000);