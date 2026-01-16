jQuery(document).ready(function($) {
 
    var checkpointId = getUrlVars()["checkpoint"];
    if (checkpointId != undefined) {
         localStorage.setItem('checkpointId', checkpointId);
    }
    else {
        checkpointId = localStorage.getItem('checkpointId');
        if (checkpointId == undefined) {
            checkpointId = "1";
        }
    }

    var queueStatus = "";

    var es = new EventSource('/transport/alerts/stream');
    es.onmessage = function (event) {

        var data = JSON.parse(event.data);

      if (data.checkpointId == checkpointId && data.congestion && data.congestion != queueStatus) {
        queueStatus = data.congestion;

        if (queueStatus == "high") {
            Push.create('AIRPORT LONG QUEUE', {
                body: 'Expect a long queue at the airport today, so get there extra early.',
                icon: 'images/airport-icon-warning.png',
                timeout: 8000,               // Timeout before notification closes automatically.
                vibrate: [100, 100, 100],    // An array of vibration pulses for mobile devices.
                onClick: function() {
                    // Callback for when the notification is clicked. 
                    console.log(this);
                }  
            });

            $("#queue-normal").fadeOut();
            $("#queue-warning").fadeIn();
        }
        else {
            Push.create('AIRPORT queue shortened', {
                body: "AIRPORT queue has shortened, you're in luck, little waiting expected.",
                icon: 'images/airport-icon.png',
                timeout: 8000,               // Timeout before notification closes automatically.
                vibrate: [100, 100, 100],    // An array of vibration pulses for mobile devices.
                onClick: function() {
                    // Callback for when the notification is clicked. 
                    console.log(this);
                }  
            });  

            $("#queue-warning").fadeOut();    
            $("#queue-normal").fadeIn();             
        }
    }   
    };

});

function getUrlVars() {
    var vars = {};
    var parts = window.location.href.replace(/[?&]+([^=&]+)=([^&]*)/gi, function(m,key,value) {
        vars[key] = value;
    });
    return vars;
}