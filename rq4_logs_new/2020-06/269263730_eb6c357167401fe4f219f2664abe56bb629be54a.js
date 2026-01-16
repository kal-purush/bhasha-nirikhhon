"use strict";


class Trip {
    constructor(tripStart, tripEnd, tripType, tripLength) {
        this.tripStart = tripStart;
        this.tripEnd = tripEnd;
        this.tripType = tripType;
        this.tripLength = (tripEnd - tripStart).toFixed(2);
    }
}

class TripLogger {
    constructor() {
        this.trips = [
            // new Trip("2389", "2431", "business"),
            // new Trip("2431", "2490", "personal")
        ];
    }

    addTrip(info) {
        this.trips = [...this.trips, info];
    }

    showTrips() {
        document.querySelector(".list").innerHTML = "";
        let count = 0;
        for (let trip of this.trips) {
            const newEntry = document.createElement("li");
            newEntry.setAttribute("index", count);
            newEntry.innerHTML = `
                ${count +1}: 
                Trip Start: ${trip.tripStart},
                Trip End: ${trip.tripEnd},
                Trip Length: ${trip.tripLength} miles,
                Trip Type: ${trip.tripType}`
                ;
            document.querySelector(".list").append(newEntry);
            count++;
        }
    }
}


    const tripLog = new TripLogger();

    document.querySelector(".iconBtn").addEventListener("click", openApp);
    document.querySelector(".backBtn").addEventListener("click", goBack);
    document.querySelector(".startBtn").addEventListener("click", startOdometer);
    document.querySelector(".endBtn").addEventListener("click", endOdometer);
    document.querySelector(".saveBtn").addEventListener("click", saveTrip);
    // document.querySelector(".tripType").classList.add("hide");
    // document.querySelector(".saveBtn").classList.add("hide");
    document.querySelector("main").classList.add("hide");

    function openApp() {
        document.querySelector(".landingSection").classList.add("hide");
        document.querySelector("main").classList.remove("hide");
    }

    function goBack() {
        document.querySelector("main").classList.add("hide");
        document.querySelector(".landingSection").classList.remove("hide");
    }

    let oneTenth = 0;
    let mile = 0;
    let odometer = document.querySelector(".odometer");
    let odometerRead;
    let interval;
    function startOdometer() {
        odometer.classList.remove("hide");
        if (odometerRead == null) {
            odometerRead = 0;
        } else {
            odometerRead = odometer.innerHTML;
        }
        // console.log(`start: ${odometerRead}`);
        interval = setInterval(function () {
            odometer.innerHTML = mile + "." + oneTenth;
            oneTenth++;
            if (oneTenth == 10) {
                mile++;
                oneTenth = 0;
            }
        }, 350);
    }

    let odometer2Read;
    function endOdometer() {
        clearInterval(interval);
        odometer2Read = odometer.innerHTML;
        // console.log(`end: ${odometer2Read}`);
        // document.querySelector(".tripType").classList.remove("hide");
        // document.querySelector(".saveBtn").classList.remove("hide");    
    }

    function saveTrip(event) {
        event.preventDefault();
        // console.log(`save start: ${odometerRead}`);
        // console.log(`save end: ${odometer2Read}`);
        let selection = document.getElementsByName("choice");
        let selected;      
        for (let i=0; i<selection.length; i++) {
            if (selection[i].checked) {
                selected = selection[i].value;    
            }
        }    
        // console.log(`save choice: ${selected}`);
        const newTrip = new Trip(
            odometerRead,
            odometer2Read,
            selected
        );
        tripLog.addTrip(newTrip);
        tripLog.showTrips();
        // document.querySelector(".odometer").classList.add("hide");
        // document.querySelector(".tripType").classList.add("hide");
        // document.querySelector(".saveBtn").classList.add("hide");
    }