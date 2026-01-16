
    fetch("json/data.json")
    .then(function (response){
     return response.json();
     console.log(data);
    patcheventName(data);
    })
   
    
    .then(function(data) {
      
        document.getElementById("event-1").innerHTML += data[4].eventName ;
        document.getElementById("event-2").innerHTML += data[3].eventName;
        document.getElementById("event-3").innerHTML += data[2].eventName;
        document.getElementById("event-4").innerHTML += data[1].eventName;
        document.getElementById("event-5").innerHTML += data[0].eventName;

        document.getElementById("date-1").innerHTML +=data[4].date;
        document.getElementById("date-2").innerHTML +=data[3].date;
        document.getElementById("date-3").innerHTML +=data[2].date;
        document.getElementById("date-4").innerHTML +=data[1].date;
        document.getElementById("date-5").innerHTML +=data[3].date;

        document.getElementById("abt-1").innerHTML +=data[4].description;
        document.getElementById("abt-2").innerHTML +=data[3].description;
        document.getElementById("abt-3").innerHTML +=data[2].description;
        document.getElementById("abt-4").innerHTML +=data[1].description;
        document.getElementById("abt-5").innerHTML +=data[0].description;
      
        document.getElementById("img-1").src = data[4].image;
        document.getElementById("img-2").src = data[3].image;
        document.getElementById("img-3").src = data[2].image;
        document.getElementById("img-4").src = data[1].image;
        document.getElementById("img-5").src = data[0].image;
    });
    function patcheventName(data){
        document.getElementById("event-3").innerHTML += data[2].eventName;
        
    }
    function patchdescription(data){
        document.getElementById("abt-1").innerHTML += data[4].description;
    }
    function patchspeaker(data){

        document.getElementById("speaker-1").innerHTML += data[0].speaker;
    }
    function patchspeakerImg(data){

        document.getElementById("img-1").innerHTML += data[0].image;
    }
    function navigationpage1() {
        window.location.href="http://127.0.0.1:5500/eventdescription.html?id=1";
        
    }
    function navigationpage2() {
        window.location.href="http://127.0.0.1:5500/eventdescription.html?id=2";
        
    }
    function navigationpage3() {
        window.location.href="http://127.0.0.1:5500/eventdescription.html?id=3";
        
    }
    function navigationpage4() {
        window.location.href="http://127.0.0.1:5500/eventdescription.html?id=4";
        
    }
    function navigationpage5() {
        window.location.href="http://127.0.0.1:5500/eventdescription.html?id=5";
        
    }