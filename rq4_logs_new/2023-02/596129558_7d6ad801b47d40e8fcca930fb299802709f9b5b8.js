// var Authorization = {
//     key: 'CWB-69660E83-08F8-4D6D-A365-8AFB7E0C6A88'
// }
// function getweather() {
//     fetch("https://opendata.cwb.gov.tw/api/v1/rest/datastore/F-D0047-091?Authorization=" + Authorization.key + "&format=JSON&locationName=%E8%87%BA%E5%8C%97%E5%B8%82&elementName=Wx").then(function (response) {
//         return response.json();
//     }).then(function (data) {
//         $('#morning').empty();
//         $('#night').empty();
//         var wxtime = data.records.locations[0].location[0].weatherElement[0].time;
//         for (let i = 0; i < wxtime.length; i++) {
//             const wx = wxtime[i];
//             console.log(wx.startTime+" "+wx.elementValue[0].value);
//             // console.log(wx.startTime.substr(8, 2) + " " + wx.elementValue[0].value);
//             // if(wx.startTime.substr(11,2)=="06"){
//             //     $('#morning').append("<br>"+wx.startTime.substr(8, 2) + " " + wx.elementValue[0].value);
//             // }else{
//             //     $('#night').append("<br>"+wx.startTime.substr(8, 2) + " " + wx.elementValue[0].value);
//             // }
//         }
//     })
// }

const key="ef9059f5a1bf2bbe3dc3d337b3f15ece";

const notificationElemnt=document.querySelector(".notification");
const iconElement=document.querySelector(".weather-icon");
const tempElement=document.querySelector(".temperature-value p");
const tempdescElement=document.querySelector(".temperature-description p");
const locationElement=document.querySelector(".location");


const weather={
    temperature:{
        value:18,
        unit:"celsius"
    },
    description:"few clouds",
    iconId:"01d",
    city:"London"
}
function getweather(lat,lon){
    let api=`https://api.openweathermap.org/data/2.5/weather?lat=${lat}&lon=${lon}&appid=${key}`
    console.log(api);
    fetch(api).then(function(response){
        return response.json()
    }).then(function(data){
        console.log(data);
        weather.temperature.value=Math.floor(data.main.temp-273.15);
        weather.description=data.weather[0].description;
        weather.city=data.name;
        displayWeather()
    })
}
function displayWeather(){
    iconElement.innerHTML=`<img src="${weather.iconId}.png"></img>`;
    tempElement.innerHTML=`${weather.temperature.value}<span>C</span>`;
    tempdescElement.innerHTML=weather.description;
    locationElement.innerHTML=weather.city;
}
if("geolocation"in navigator){
    navigator.geolocation.getCurrentPosition(setPosition,showError);
}else{
    notificationElemnt.style.display="block";
    notificationElemnt.innerHTML="<p>Browser doesn't support Geolocation</p>"
}

function setPosition(position){
    let latitude = position.coords.latitude;
    let longitude=position.coords.longitude;
    getweather(latitude,longitude);
}
function showError(error){
    notificationElemnt.style.display="block";
    notificationElemnt.innerHTML=`<p>${error.message}</p>`;
}
