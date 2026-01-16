let apiKey='09d8a2f896b15149a42abca2aabd354b';
let apiUrl = 'https://api.openweathermap.org/data/2.5/weather?units=metric&q=';

let searchBox =document.querySelector('.search-box input');
let searchBtn =document.querySelector('.search-box button');
const weatherIcon=document.querySelector('.weather-icon');


async function checkWeatherData(city){
    const response = await fetch(`${apiUrl}${city}&appid=${apiKey}`);
    // the line starts asynchronous operation using fetch() function to make an HTTP request to a weather API endpoint.
    // 'await' make sure that the functiom waits until the promise returned by fetch();
    if(response.status==404){
        document.querySelector(".error").style.display="block";
        document.querySelector(".weather").style.display="none";
    }else{ 
        //parsing data
        var data=await response.json();
        console.log(data); 
        //it is for fetching data for the info below

    
    //open console and find the information related to the city(mostly in main field)
    document.querySelector(".city").innerHTML= data.name;
    document.querySelector(".temp").innerHTML= Math.round(data.main.temp) + "°C"; 
    document.querySelector(".humidity").innerHTML= data.main.humidity+ "%";
    document.querySelector(".wind").innerHTML= data.wind.speed + " km/h "; //this info is not within main block
    //updating sources of img,dependig on the weather in console
    if(data.weather[0].main=="Clouds"){
        weatherIcon.src="images/clouds.png";
    }else if(data.weather[0].main=="Clear"){
        weatherIcon.src="images/clear.png";
    }else if(data.weather[0].main=="Rain"){
        weatherIcon.src="images/rain.png";
    }else if(data.weather[0].main=="Drizzle"){
        weatherIcon.src="images/drizzle.png";
    } else if(data.weather[0].main=="Mist"){
        weatherIcon.src="images/mist.png";
}
//to display no error in the beginning
document.querySelector(".weather").style.display="block";
document.querySelector(".error").style.display="none";
   
}}
//here we pass that the city in the function above is the value of searchbox+ add eventlistener to make an action when it is clicked.
searchBtn.addEventListener("click",()=>{
    checkWeatherData(searchBox.value);
})