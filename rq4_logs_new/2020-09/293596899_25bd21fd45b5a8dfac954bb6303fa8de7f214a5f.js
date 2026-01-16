const request = new XMLHttpRequest();

const APIKey = "2de849ba4967069451302d2dea7b66ad";


let handleSubmit = () => {
    let cityName = document.getElementById("CityName").value;
    let unit = "metric";
    const url = "https://api.openweathermap.org/data/2.5/weather?q=" + cityName + "&appid=" + APIKey + "&units=" + unit;

    request.open("GET", url, true);
    request.send();
    let name, temp, description;

    request.onload = function() {
        let responseData = JSON.parse(this.response);
        name = responseData.name;
        temp = responseData.main.temp;
        description = responseData.weather[0].main;
        
        document.querySelector("#name").textContent = name;
        document.querySelector("#temp").textContent = "Temperature: " + temp;
        document.querySelector("#desc").textContent = "Description: " +  description;
    }
    
}



