// fetch("https://restcountries.eu/rest/v2/all")
// fetch("https://restcountries.eu/rest/v2/region/europe")

// Global Variable
const countriesList = document.getElementById("countries");
let countries; // Contain fetched data

//-------------------------------------
// Event Listener for changing 
// countrie mapping throw all european countries
//-------------------------------------
countriesList.addEventListener("change", function(event) {
    // console.log(event.target.value);
    displayCountryInfo(event.target.value);
});

//-------------------------------------
// Fetch request to the 
// API with FETCH Method
//-------------------------------------
fetch("https://restcountries.eu/rest/v2/region/europe")
    .then(response => response.json())
    .then(data => initialize(data))
    .catch(err => console.log("Error:", err));

//-------------------------------------
//MAke a Function to 
//recive the data from API
//-------------------------------------
function initialize(countriesData) {
    // console.log(countriesData);
    countries = countriesData;
    let options = "";

//-------------------------------------
//Loop throw each countrie 
// data and get the value
//-------------------------------------
    countries.forEach(country =>
        options += `<option value="${country.alpha3Code}">${country.name}</option>`);
    countriesList.innerHTML = options;
    displayCountryInfo("ALA")
}
//-------------------------------------
// Get the id and selector 
// in HTML for output in browser
//-------------------------------------
function displayCountryInfo(countryByAlfa3Code) {
    const countryData = countries.find(country => country.alpha3Code === countryByAlfa3Code);
    // console.log(countryData);
    document.querySelector("#flag-container img").src = countryData.flag;
    document.querySelector("#flag-container img").alt = `Flag of ${countryData.name}`;
    document.getElementById("capital").innerHTML = countryData.capital;
    document.getElementById("dialing-code").innerHTML = `+${countryData.callingCodes[0]}`;
    document.getElementById("population").innerHTML = countryData.population.toLocaleString("en-US");
    document.getElementById("languages").innerHTML = countryData.languages.filter(c => c.name).map(c => `${c.name} (${c.nativeName})`).join(", ");
    document.getElementById("currencies").innerHTML = countryData.currencies.filter(c => c.name).map(c => `${c.name} (${c.code})`).join("---");
    document.getElementById("region").innerHTML = `${countryData.region}`;
    document.getElementById("sub-region").innerHTML = `${countryData.subregion}`;
}


