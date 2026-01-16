// Adafruit IO REST API URL
const API_URL = `https://io.adafruit.com/api/v2/coolguy_morin4/feeds/temperature`;
//const API_URL = 'https://io.adafruit.com/api/v2/coolguy_morin4/feeds/temperature/data?limit=1';

// Fetch data from Adafruit IO
fetch(API_URL)
.then(response => {
    if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
    }
    return response.json();
})
.then(data => {
    console.log("Fetched Data:", data); // Print data to console
    // Optionally display data on a webpage
    displayTemp(data)
    //document.getElementById("temperature").innerText = JSON.stringify(data["last_value"], null, 2);
})
.catch(error => {
    console.error("Error fetching data:", error);
});

function displayTemp(data){
    const temp = data["last_value"];
    const tempDiv = document.getElementById("temperature");

    const heading = document.createElement("h6");
    heading.innerHTML = "Temperature = " + temp + " degrees Celcius";
    tempDiv.appendChild(heading);

}
// Adafruit IO REST API URL
const API_URL2 = `https://io.adafruit.com/api/v2/coolguy_morin4/feeds/humidity`;
//const API_URL = 'https://io.adafruit.com/api/v2/coolguy_morin4/feeds/temperature/data?limit=1';

// Fetch data from Adafruit IO
fetch(API_URL2)
.then(response => {
    if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
    }
    return response.json();
})
.then(data2 => {
    console.log("Fetched Data:", data2); // Print data to console
    // Optionally display data on a webpage
    displayHum(data2)
    //document.getElementById("temperature").innerText = JSON.stringify(data["last_value"], null, 2);
})
.catch(error => {
    console.error("Error fetching data:", error);
});

function displayHum(data2){
    const hum = data2["last_value"];
    const humDiv = document.getElementById("humidity");

    const heading = document.createElement("h6");
    heading.innerHTML = "Humidity = " + hum + " %";
    humDiv.appendChild(heading);

}
// Adafruit IO REST API URL
const API_URL3 = `https://io.adafruit.com/api/v2/coolguy_morin4/feeds/pressure`;
//const API_URL = 'https://io.adafruit.com/api/v2/coolguy_morin4/feeds/temperature/data?limit=1';

// Fetch data from Adafruit IO
fetch(API_URL3)
.then(response => {
    if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
    }
    return response.json();
})
.then(data3 => {
    console.log("Fetched Data:", data3); // Print data to console
    // Optionally display data on a webpage
    displayPress(data3)
    //document.getElementById("temperature").innerText = JSON.stringify(data["last_value"], null, 2);
})
.catch(error => {
    console.error("Error fetching data:", error);
});

function displayPress(data3){
    const press = data3["last_value"];
    const pressDiv = document.getElementById("pressure");

    const heading = document.createElement("h6");
    heading.innerHTML = "Pressure = " + press + " ";
    pressDiv.appendChild(heading);

}