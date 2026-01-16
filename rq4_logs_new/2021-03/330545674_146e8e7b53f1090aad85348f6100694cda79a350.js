/*
    Name: Dylan Mackey
    Date: 3/12/2021
    Assignment: Smart Home Simulator
    Filename: script.js
*/

/* Running Scripts */
var devices = new Map();

//when DOM is loaded
window.addEventListener('DOMContentLoaded', (event) => {
    //initialize devices
    if (getDevices() == null) {
        initializeDevices();
        saveDevices();
    } else {
        devices = getDevices();
    }

    //add devices to DOM
    createDevices();

    //add click events
    addEvents();
});

function addEvents() {
    //set up event delegation
    let save = document.getElementById("save");
    save.addEventListener("click", (e) => {
        //variables for validation
        var cL;
        let startHour = document.getElementById("startHour");
        let startMinutes = document.getElementById("startMinutes");
        let startMed = document.getElementById("startMed");
        let endHour = document.getElementById("endHour");
        let endMinutes = document.getElementById("endMinutes");
        let endMed = document.getElementById("endMed");
        let valid = true;

        //start hour
        cL = document.getElementById("badStartHour").classList;
        if (startHour.value > 12 || startHour.value < 1) {
            valid = false;
            startHour.style.border = "1px solid #dc3545";
            cL.add('d-block');
        } else {
            cL.remove('d-block');
            startHour.removeAttribute('style');
        }

        //start minutes
        cL = document.getElementById("badStartMinutes").classList;
        if (startMinutes.value > 59 || startMinutes.value < 0 || startMinutes.value.length == 0) {
            valid = false;
            startMinutes.style.border = "1px solid #dc3545";
            cL.add('d-block');
        } else {
            cL.remove('d-block');
            startMinutes.removeAttribute('style');
        }
        //end hour
        cL = document.getElementById("badEndHour").classList;
        if (endHour.value > 12 || endHour.value < 1) {
            valid = false;
            endHour.style.border = "1px solid #dc3545";
            cL.add('d-block');
        } else {
            cL.remove('d-block');
            endHour.removeAttribute('style');
        }

        //end minutes
        cL = document.getElementById("badEndMinutes").classList;
        if (endMinutes.value > 59 || endMinutes.value < 0 || endMinutes.value.length == 0) {
            valid = false;
            endMinutes.style.border = "1px solid #dc3545";
            cL.add('d-block');
        } else {
            cL.remove('d-block');
            endMinutes.removeAttribute('style');
        }

        if (valid) {
            //update device
            let startingHour = +startHour.value;
            let endingHour = +endHour.value;
            if (startingHour == 12 && +startMed.value == 0) {
                startingHour -= 12;
            }
            if (endingHour == 12 && +endMed.value == 0) {
                endingHour -= 12;
            }
            let device = devices.get(document.getElementById("modalTitle").value);
            device.start = (startingHour + +startMed.value) + ":" + startMinutes.value;
            device.end = (endingHour + +endMed.value) + ":" + endMinutes.value;
            saveDevices();

            //play animation
            save.classList.add("btn-success");
            save.classList.add("px-5");
            save.style.transition = "1s";
            save.classList.remove("btn-primary");
            save.innerHTML = "&#10004;";
            setTimeout(function () {
                save.classList.add("btn-primary");
                save.classList.remove("btn-success");
                setTimeout(function () {
                    save.style.transition = "0s";
                    save.classList.remove("px-5");
                    save.innerHTML = "Save Changes";
                }, 1000);
            }, 1000);
        } else {
            //play animation
            save.classList.add("btn-danger");
            save.classList.add("px-5");
            save.style.transition = "1s";
            save.classList.remove("btn-primary");
            save.innerHTML = "&times;";
            setTimeout(function () {
                save.classList.add("btn-primary");
                save.classList.remove("btn-danger");
                setTimeout(function () {
                    save.style.transition = "0s";
                    save.classList.remove("px-5");
                    save.innerHTML = "Save Changes";
                }, 1000);
            }, 1000);
        }
    });
}

function createDevices() {
    //get container
    var container = document.getElementById("container");

    //make a row
    var row = document.createElement("DIV");
    row.classList.add("row");
    row.classList.add("align-items-end");
    row.classList.add("justify-content-center");
    row.classList.add("w-100");
    row.classList.add("mx-auto");

    //add devices
    devices.forEach(device => {
        //make a col
        let col = document.createElement("DIV");
        col.classList.add("col-3");

        //make a box
        let box = document.createElement("DIV");
        box.classList.add("device");
        box.classList.add("pb-5");
        box.setAttribute("data-toggle", "modal");
        box.setAttribute("data-target", "#timeChange");

        //add image
        let img = document.createElement("IMG");
        img.setAttribute("src", "img/" + device.key + ".png");
        img.setAttribute("alt", device.name);
        img.setAttribute("data-toggle", "tooltip");
        img.setAttribute("title", "Click to Change Times.");
        img.setAttribute("width", 200);
        img.classList.add("mx-auto");
        img.classList.add("d-block");
        img.style.maxHeight = "270px";

        //add title
        let title = document.createElement("h4");
        title.setAttribute("data-toggle", "tooltip");
        title.setAttribute("title", "Click to Change Times.");
        title.innerHTML = device.name;
        title.style.textAlign = "center";

        //add event listener to box
        box.addEventListener('click', (e) => {
            //set header
            let header = document.getElementById("modalTitle");
            header.innerHTML = device.name;
            header.value = device.key;

            //get times
            if (device.start != "" || device.end != "") {
                //get variables
                let startTime = device.start.split(":");
                let endTime = device.end.split(":");
                let isStartMorning = (startTime[0] - 12 <= 0) ? true : false;
                let isEndMorning = (endTime[0] - 12 <= 0) ? true : false;

                //set times
                let hours = +startTime[0];
                let minutes = +startTime[1];
                let isMorning = isStartMorning;

                document.getElementById("startHour").value = ((isMorning) ? /*morning*/ ((hours == 0) ? 12 : hours) : /*night*/ ((hours == 12) ? hours : hours - 12));
                document.getElementById("startMinutes").value = ((minutes.toString().length < 2) ? "0" + minutes : minutes);
                document.getElementById("startMed").selectedIndex = (isMorning) ? "0" : "1"

                hours = +endTime[0];
                minutes = +endTime[1];
                isMorning = isEndMorning;
                document.getElementById("endHour").value = ((isMorning) ? /*morning*/ ((hours == 0) ? 12 : hours) : /*night*/ ((hours == 12) ? hours : hours - 12));
                document.getElementById("endMinutes").value = ((minutes.toString().length < 2) ? "0" + minutes : minutes);
                document.getElementById("endMed").selectedIndex = (isMorning) ? "0" : "1"
            } else {
                //update for no times for device
                document.getElementById("startHour").value = "";
                document.getElementById("startMinutes").value = "";
                document.getElementById("startMed").selectedIndex = "0";
                document.getElementById("endHour").value = "";
                document.getElementById("endMinutes").value = "";
                document.getElementById("endMed").selectedIndex = "0";
            }

            //add focus
            $('#timeChange').on('shown.bs.modal', function () {
                $('#startHour').focus();
            });
        });

        //connect objects
        box.appendChild(img);
        box.appendChild(title);
        col.appendChild(box);
        row.appendChild(col);
    });

    //add row to container
    container.appendChild(row);
}

function saveDevices() {
    localStorage.setItem("devices", JSON.stringify(devices, replacer));
}

function getDevices() {
    return JSON.parse(localStorage.getItem("devices"), reviver);
}

function replacer(key, value) {
    if (value instanceof Map) {
        return {
            dataType: 'Map',
            value: Array.from(value.entries()),
        };
    } else {
        return value;
    }
}

function reviver(key, value) {
    if (typeof value === 'object' && value !== null) {
        if (value.dataType === 'Map') {
            return new Map(value.value);
        }
    }
    return value;
}

function initializeDevices() {
    devices.set("livingRoomLamp", {
        "name": "Living Room Lamp",
        "key": "livingRoomLamp",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "turns on",
        "offWord": "turns off"
    });
    devices.set("deskLamp", {
        "name": "Bedroom #1 Desk Lamp",
        "key": "deskLamp",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "turns on",
        "offWord": "turns off"
    });
    devices.set("bathroomFan", {
        "name": "Bathroom Fan",
        "key": "bathroomFan",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "turns on",
        "offWord": "turns off"
    });
    devices.set("washer", {
        "name": "Washer",
        "key": "washer",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "starts",
        "offWord": "ends"
    });
    devices.set("dryer", {
        "name": "Dryer",
        "key": "dryer",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "starts",
        "offWord": "ends"
    });
    devices.set("floorLamp", {
        "name": "Bedroom #2 Floor Lamp",
        "key": "floorLamp",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "turns on",
        "offWord": "turns off"
    });
    devices.set("frontDoor", {
        "name": "Front Door",
        "key": "frontDoor",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "locks",
        "offWord": "unlocks"
    });
    devices.set("backDoor", {
        "name": "Back Door",
        "key": "backDoor",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "locks",
        "offWord": "unlocks"
    });
    devices.set("garageDoor", {
        "name": "Garage Door",
        "key": "garageDoor",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "closes",
        "offWord": "opens"
    });
    devices.set("kitchenFan", {
        "name": "Kitchen Fan",
        "key": "kitchenFan",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "turns on",
        "offWord": "turns off"
    });
    devices.set("diningRoomLight", {
        "name": "Dining Room Light",
        "key": "diningRoomLight",
        "start": "",
        "end": "",
        "isOn": null,
        "onWord": "turns on",
        "offWord": "turns off"
    });
}