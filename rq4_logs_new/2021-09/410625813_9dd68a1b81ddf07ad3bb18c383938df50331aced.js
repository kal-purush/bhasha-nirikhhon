import config from './config.js';

const api_key = config.API_KEY;

// GETTING CURRENT TIME
const getCurrentTime = () => {
    let currentDate = new Date();
    let epoch = currentDate.getTime();

    //Calendar date

    let year = currentDate.getFullYear();
    let month = currentDate.getMonth() + 1;
    let day = currentDate.getDate();

    //Day time
    let hour = currentDate.getUTCHours() + 2;
    let ampm = hour < 12 ? 'AM' : 'PM';
    let minutes = currentDate.getMinutes < 10 ? `0${currentDate.getMinutes()}` : currentDate.getMinutes();
    let seconds = currentDate.getSeconds() < 10 ? `0${currentDate.getSeconds()}` : currentDate.getSeconds();
    let weekDay = currentDate.getDay();

    return {
        epoch: epoch,
        hour: hour,
        minutes: minutes,
        seconds: seconds,
        year: year,
        month: month,
        day: day,
        weekday: weekDay,
        ampm: ampm,
    }
}

// GETTING CURRENT DATE

const getCurrentDate = () => {
    const currentDate = new Date();
    const options = { weekday: 'long', day: 'numeric', year: 'numeric', month: 'long' };
    const processedDate = currentDate.toLocaleDateString('en-EN', options);

    return processedDate;

}

// CONVERTING UNIX TIMESTAMP TO DAYWEEK

const convertToDayWeek = (milliseconds) => {

    const ms = milliseconds * 1000;
    const dayWeek = new Date(ms).toLocaleDateString('en-US', { weekday: 'long' }).slice(0, 3).toUpperCase();

    return dayWeek;

}

const displayCurrentDate = () => {

    setInterval(() => {
        let { hour, minutes, ampm } = getCurrentTime();
        const time = document.querySelector('.time__current');
        const currentTime = document.querySelector('.header__current-date');

        time.style.fontSize = "20px";
        time.innerText = `${hour <= 12 ? hour : hour - 12}:${minutes < 10 ? `0${minutes}` : minutes} ${ampm}`;
        currentTime.style.fontSize = "11px";
        currentTime.innerText = getCurrentDate();
    }, 1000);
}

//LOAD CITIES DATA INCLUDING COORDS

const fetchCitiesData = async () => {
    try {
        let response = await fetch('../cities.json');
        let data = response.json();

        return data
    } catch (err) {
        console.log(err)
    }

}

//FORMAT USER'S INPUT TO MATCH CITY LIST

const convertLocation = (location) => {

    let arr = location.split(" ");

    for (let i = 0; i < arr.length; i++) {

        arr[i] = arr[i].substring(0, 1).toUpperCase() + arr[i].substr(1, arr[i].length);
    }

    return arr.join(" ");


}

//GET CITY COORDS CHOSEN BY USER IN SEARCH FIELD

const getCity = async (location) => {

    if (!location) return;

    let output = "";
    let cities = await fetchCitiesData();
    const processedLocation = convertLocation(location);
    const filteredCities = cities.filter(city => city.name == processedLocation);

    if (filteredCities.length === 0) {
        alert('Bad location, please try once again')
    };

    //CREATE CITIES LIST PROMPT 

    const searchContainer = document.querySelector('.search__wrapper');
    const cityList = document.querySelector('.search__dropdown-menu');

    if (cityList) {
        cityList.parentNode.removeChild(cityList);
    };

    const ul = document.createElement('ul');

    ul.classList.add('search__dropdown-menu');

    const createCityRowHtml = (city) => {
        return `
    <li class="search__city-row"><span class="search__city">${city.name}, ${city.country}</span><span class="search__coords">${Number(city.lat).toFixed(4)}, ${Number(city.lng).toFixed(4)}</span></li>
    `}

    filteredCities.forEach(city => {
        output += createCityRowHtml(city);
    });


    ul.innerHTML = output;
    searchContainer.appendChild(ul);



    ul.addEventListener('click', async (e) => {
        let coords;

        e.target.tagName === 'SPAN' ? coords = e.target.parentNode.lastChild.textContent : coords = e.target.lastChild.textContent;

        const processedCoords = coords.split(',');
        const lat = processedCoords[0];
        const lng = processedCoords[1];



        console.log(lat, lng);

        const locationCoords = {
            lat: Number(lat),
            lng: Number(lng)
        };

        console.log(locationCoords);

        const locationData = await fetchData(locationCoords);

        console.log(locationData);
        const listDropdownMenu = document.querySelector('.search__dropdown-menu');
        listDropdownMenu.style.display = "none";
        displayWeatherData(locationData);

        //CLEAR INPUT VALUE

        const input = document.querySelector('.search__input');
        input.value = "";

    });

    document.querySelector('.search__input').style.borderBottomLeftRadius = '0px';
    document.querySelector('.search__input').style.borderBottomRightRadius = '0px';
    document.querySelector('.search__input').style.boxShadow = 'none';

}

const searchButton = document.querySelector('.search__button');

searchButton.addEventListener('click', function (e) {
    const input = document.querySelector('.search__input').value;
    getCity(input);
})

// FETCH API_URL

const fetchData = async (location) => {

    try {

        // USING ONE CALL API

        let response = await fetch(`https://api.openweathermap.org/data/2.5/onecall?lat=${location.lat}&lon=${location.lng}&units=metric&exclude=minutely,hourly,alerts&appid=${api_key}`)
        let data = await response.json();

        // console.log(data);

        return data;
    } catch (err) {
        console.log(err);
    }

}


const getCoords = async () => {
    const pos = await new Promise((res, rej) => {
        navigator.geolocation.getCurrentPosition(res, rej);
    });


    console.log(pos);

    return {
        lat: Number(pos.coords.latitude.toFixed(4)),
        lng: Number(pos.coords.longitude.toFixed(4))
    }

}


//CREATE DAY-ROW WEATHER TEMPLATE

const showNextDaysWeather = (el) => {
    return `<li class="upcomming-forecast__row">
   <span class="upcomming-forecast__day">${convertToDayWeek(el.dt)}</span>
     <div class="upcomming-forecast__values">
     <img src="/temporaryicons/cloudy.svg" alt="" class="upcomming-forecast__image">
     <span class="upcomming-forecast__temperatures">${Math.round(el.temp.max)}°/${Math.round(el.temp.min)}°</span>
     </div>
     </li>`
}

// RENDERING NEXT DAYS WEATHER

const renderNextDaysWeather = (data) => {
    const nexDayWeatherData = data.daily.slice(1, 8);
    let list = document.querySelector('.card__forecast-list');
    let output = "";

    nexDayWeatherData.forEach(element => {
        output += showNextDaysWeather(element);

    });

    //APPEND DATA TO HTML LIST

    list.innerHTML = output;

}

// DISPLAYING MAIN DATA FROM API TO THE USER

const displayWeatherData = (data) => {

    const temperature = document.querySelector('.card__temperature');
    const lowestTemperature = document.querySelector('.card__lowest-temperature');
    const highestTemperature = document.querySelector('.card__highest-temperature');
    const weatherDescription = document.querySelector('.card__weather');
    const humidity = document.querySelector('.card__humidity-value');
    const windSpeed = document.querySelector('.card__wind-speed');

    temperature.innerText = Math.round(data.current.temp);
    lowestTemperature.innerText = Math.round(data.daily[0].temp.min);
    highestTemperature.innerText = Math.round(data.daily[0].temp.max);
    humidity.innerText = `${data.current.humidity}%`;
    windSpeed.innerText = `${data.current.wind_speed}m/s`

    const firstLetterToUpperCase = (str) => {
        return str.charAt(0).toUpperCase() + str.slice(1);
    };
    weatherDescription.innerText = firstLetterToUpperCase(data.current.weather[0].description);

    renderNextDaysWeather(data);

};

window.addEventListener('DOMContentLoaded', async (e) => {

    //CLEAR INPUT VALUE

    const input = document.querySelector('.search__input');
    input.value = "";

    // RENDER CURRENT DATE

    displayCurrentDate();

    //SET USER COORDS USING GEOLOCATION AND FETCH DATA

    const coords = await getCoords();
    const locationData = await fetchData(coords);

    //RENDER WEATHER DATA

    displayWeatherData(locationData);
});
