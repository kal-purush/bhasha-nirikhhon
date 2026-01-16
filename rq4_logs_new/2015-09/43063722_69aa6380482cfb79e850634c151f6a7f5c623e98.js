
    
    var timeOnStart;
    var timeOnStop;
 
    function StartTimer(){
      document.getElementById("myDiv").innerHTML="Experiment started."
      timeOnStart = new Date().getTime(); 
    }

      function loadXMLDoc(callerName) {
        var xmlhttp;
        timeOnStop = new Date().getTime();
        var interval = timeOnStop - timeOnStart;
        if (window.XMLHttpRequest){
          xmlhttp=new XMLHttpRequest();
        }
        else{
          xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");
        }
        xmlhttp.onreadystatechange = function(){
          if (xmlhttp.readyState==4 && xmlhttp.status==200){
            document.getElementById("myDiv").innerHTML=xmlhttp.responseText;
          }
        }
        xmlhttp.open("GET","server.php?interval=" + interval + "&name=" + callerName, true);
        xmlhttp.send();
      }
      
