/* global $ */
/* global firebase */


// Initialize Firebase
var config = {
    apiKey: "AIzaSyAAi7s87SW5XOtn5dKwIBwEIZsY4tBZfkg",
    authDomain: "ryde-or-dryve.firebaseapp.com",
    databaseURL: "https://ryde-or-dryve.firebaseio.com",
    projectId: "ryde-or-dryve",
    storageBucket: "ryde-or-dryve.appspot.com",
    messagingSenderId: "998231386924"
};
firebase.initializeApp(config);

var startLocation = "";
var destination = "";
var mpg = "";
var startLat;
var startLng;
var destLat;
var destLng;



$("#display").hide();
$("#loading").hide()
$("#locationcheck").on("click", function() {
    if ($("#locationcheck").prop("checked")) {
        console.log("checked")
        $("#startLocationParent").hide()
    };
    if (!$("#locationcheck").prop("checked")) {
        console.log("unchecked")
        $("#startLocationParent").show()
    }
})


$("#submit").on("click", function(event) {
    event.preventDefault();
    startLocation = $("#startLocation").val().trim();
    destination = $("#destination").val().trim();
    mpg = $("#mpg").val();

    if (((startLocation !== "") || ($("#locationcheck").prop("checked"))) && (destination !== "") && (mpg !== "")) {

        // waits for getStartLatLong to finish
        if ($("#startLocation").val() !== "") {
            $("#main").hide();
            $("#loading").show();
            getStartLatLong().done(function() {
                console.log("click: " + startLat + ", " + startLng);
                getDestLatLong().done(function() {
                    console.log("click: " + destLat + ", " + destLng);
                    uberInfo(startLat, startLng, destLat, destLng);
                    lyftInfo(startLat, startLng, destLat, destLng);
                    $("#loading").hide()
                    $("#display").show()
                });
            });
        }
        else {
            $("#main").hide();
            $("#loading").show();
            getLocation().done(function() {
                console.log("click: " + startLat + ", " + startLng);
                getDestLatLong().done(function() {
                    console.log("click: " + destLat + ", " + destLng);
                    uberInfo(startLat, startLng, destLat, destLng);
                    lyftInfo(startLat, startLng, destLat, destLng);
                    $("#loading").hide()
                    $("#display").show()
                });
            });
        }
    }

    else {
        $("#message").html("Please input all information");
    }

});

//--------Geo Location Functions and Google API Calls-------------

//determines geolocation from browser

function getLocation() {
    var d = new $.Deferred();
    var showPosition = function(position) {
        startLat = position.coords.latitude;
        startLng = position.coords.longitude;
        var tmpLatLng = {
            startLat: startLat,
            startLng: startLng
        }
        d.resolve(tmpLatLng)
        console.log("my position: " + position.coords.latitude + "," + position.coords.longitude);
    };
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(showPosition);
    }
    else {
        lattitude.innerHTML = "Geolocation is not supported by this browser.";
    }
    return d.promise();
}
//console.logs the browser's latitude and longitude

// function showPosition(position) {
//     // $("#latreport").innerHTML = "Latitude: " + position.coords.latitude +
//     //     "<br>Longitude: " + position.coords.longitude;
//     // console.log("my position: " + position.coords.latitude + "," + position.coords.longitude);
//     startLat = position.coords.latitude;
//     startLng = position.coords.longitude;
// }
var getStartLatLong = function() {
    //var destinationAddress = $("#destination").val().trim();
    var geoLocURL = "https://maps.googleapis.com/maps/api/geocode/json?address=" + startLocation + "&key=AIzaSyB6P6KfBWOmNmMk9IRDVgl8OTmVtMmSEQk";
    var d = new $.Deferred();
    $.ajax({
        url: geoLocURL,
        method: "GET"
    }).done(function(results) {
        console.log("startLat: " + results.results["0"].geometry.location.lat);
        console.log("startlng: " + results.results["0"].geometry.location.lng);
        startLat = results.results["0"].geometry.location.lat;
        console.log("startLat var: " + startLat);
        startLng = results.results["0"].geometry.location.lng;
        console.log("startLng var: " + startLng);
        var tmpLatLng = {
            startLat: startLat,
            startLng: startLng
        }
        d.resolve(tmpLatLng)
    });
    return d.promise();

};
var getDestLatLong = function() {
    //var destinationAddress = $("#destination").val().trim();
    var geoLocURL = "https://maps.googleapis.com/maps/api/geocode/json?address=" + destination + "&key=AIzaSyB6P6KfBWOmNmMk9IRDVgl8OTmVtMmSEQk";
    var d = new $.Deferred();
    $.ajax({
        url: geoLocURL,
        method: "GET"
    }).done(function(results) {
        console.log("destLat: " + results.results["0"].geometry.location.lat);
        console.log("destLng: " + results.results["0"].geometry.location.lng);
        destLat = results.results["0"].geometry.location.lat;
        console.log("destLat")
        destLng = results.results["0"].geometry.location.lng;
        var tmpLatLng = {
            destLat: destLat,
            destLng: destLng
        }
        d.resolve(tmpLatLng)
    });
    return d.promise()
};

//-----------End Geo Location Functions and Google API calls

//UBER
//============================================================================================================

function uberInfo(y, x, yy, xx) { //starting latitude=x, starting longitude = y, ending latitude = xx, ending longitude = yy

    //This token will need to be updated
    var token = "KA.eyJ2ZXJzaW9uIjoyLCJpZCI6InlaL3ZIdkJnU05TUkZFeTdiUFZuQVE9PSIsImV4cGlyZXNfYXQiOjE1MTM1NjU1NTQsInBpcGVsaW5lX2tleV9pZCI6Ik1RPT0iLCJwaXBlbGluZV9pZCI6MX0.OiMPjfswSr6QI5IdKzHDro8udDWi4Ok0C5a1oKrO_kQ";

    // AJAX request for Uber's time estimate information    
    $.ajax({
        url: "https://api.uber.com/v1.2/estimates/time",
        method: "GET",
        data: {
            start_latitude: y,
            start_longitude: x,
            access_token: token
        },
        success: function(uberTimeResults) {
            console.log(uberTimeResults);
            //This loop will ensure we get the data for uberX, we can easily add another if statement to set a variable in case we want to an estimate for other types of uber
            for (var i = 0; i < uberTimeResults.times.length; i++) {
                if ("uberX" === uberTimeResults.times[i].display_name) {
                    var uberXindex = i;
                }
            }
            var minutesTilUber = Math.round(uberTimeResults.times[uberXindex].estimate / 60); //Minutes away of the closest Uber
            console.log("minutes til next uber: " + minutesTilUber);
            $("#uberETA").text(minutesTilUber)
        }
    });

    //AJAX request for Uber's price estimate information. This will also give us the distance and duration of the trip.
    $.ajax({
        url: "https://api.uber.com/v1.2/estimates/price",
        method: "GET",
        data: {
            start_latitude: y,
            start_longitude: x,
            end_latitude: yy,
            end_longitude: xx,
            access_token: token
        },
        success: function(uberPriceResults) {
            console.log(uberPriceResults);
            for (var i = 0; i < uberPriceResults.prices.length; i++) {
                if ("uberX" === uberPriceResults.prices[i].display_name) {
                    var uberXindex = i;
                }
            }
            var uberXprice = uberPriceResults.prices[uberXindex].estimate; //The range of the Uber Price Estimate
            var uberXdistance = uberPriceResults.prices[uberXindex].distance; // The distance of the trip in miles
            var uberXduration = Math.round(uberPriceResults.prices[uberXindex].duration / 60); //The minutes the trip will take
            var uberAverageXprice = Math.round((uberPriceResults.prices[uberXindex].high_estimate + uberPriceResults.prices[uberXindex].low_estimate) / 2); //The average of the range of prices in case we want Jim to get his way
            $("#ubercost").text(uberXprice)
            $("#carETA").text(uberXduration);

            //Drive Cost
            var driveDistance = uberXdistance;
            var gasPrice = 2.50;
            var driveCost = (driveDistance / mpg) * gasPrice;
            $("#gascost").text("$" + driveCost.toFixed(2))
        }
    });
}



//LYFT
//=============================================================================================================

function lyftInfo(y, x, yy, xx) {

    var lyftToken;
    var clientId = '3TY5gGnnFF3h';
    var clientSecret = 'BoWG3zPcjYe0HSqOvG4IKuIjFm5dT9hM';

    //Requests a token
    $.ajax({
        url: 'https://api.lyft.com/oauth/token',
        type: 'POST',
        data: {
            grant_type: 'client_credentials',
            scope: 'public'
        },
        beforeSend: function(xhr) {
            xhr.setRequestHeader("Authorization", "Basic " + btoa(clientId + ":" + clientSecret));
        },
        success: function(response) {
            console.log(response);
            lyftToken = response.access_token;
            console.log(lyftToken)

            //Uses token to request driver ETA information
            var lyftETAData = {
                "async": true,
                "crossDomain": true,
                "url": "https://api.lyft.com/v1/eta?lat=" + y + "&lng=" + x,
                "method": "GET",
                "headers": {
                    "authorization": "Bearer " + lyftToken
                }
            };

            $.ajax(lyftETAData).done(function(lyftTimeResults) {
                console.log(lyftTimeResults);
                for (var i = 0; i < lyftTimeResults.eta_estimates.length; i++) {
                    if ("Lyft" === lyftTimeResults.eta_estimates[i].display_name) {
                        var lyftindex = i;
                    }
                }
                var minutesTilLyft = Math.round(lyftTimeResults.eta_estimates[lyftindex].eta_seconds / 60);
                console.log("minutes til next lyft: " + minutesTilLyft);
                $("#lyftETA").text(minutesTilLyft)

            });

            //Uses token to request price estimate
            var lyftPriceData = {
                "async": true,
                "crossDomain": true,
                "url": "https://api.lyft.com/v1/cost?start_lat=" + y + "&start_lng=" + x + "&end_lat=" + yy + "&end_lng=" + xx,
                "method": "GET",
                "headers": {
                    "authorization": "Bearer " + lyftToken
                }
            };

            $.ajax(lyftPriceData).done(function(lyftPriceResults) {
                console.log(lyftPriceResults);
                for (var i = 0; i < lyftPriceResults.cost_estimates.length; i++) {
                    if ("Lyft" === lyftPriceResults.cost_estimates[i].display_name) {
                        var lyftindex = i;
                    }
                }
                var lyftAveragePrice = Math.round((lyftPriceResults.cost_estimates[lyftindex].estimated_cost_cents_min + lyftPriceResults.cost_estimates[lyftindex].estimated_cost_cents_max) / 200);
                console.log("lyft average price: " + lyftAveragePrice);
                if (lyftPriceResults.cost_estimates[lyftindex].estimated_cost_cents_min !== lyftPriceResults.cost_estimates[lyftindex].estimated_cost_cents_max) {
                    var lyftPrice = "$" + Math.round(lyftPriceResults.cost_estimates[lyftindex].estimated_cost_cents_min / 100) + "-" + Math.round(lyftPriceResults.cost_estimates[lyftindex].estimated_cost_cents_max / 100);
                }
                else {
                    var lyftPrice = "$" + Math.round(lyftPriceResults.cost_estimates[lyftindex].estimated_cost_cents_min / 100);
                }
                console.log("Price estimate for lyft: " + lyftPrice);
                $("#lyftcost").text(lyftPrice);
            });


        } //ends success

    }); //ends initial AJAX request

} //ends function