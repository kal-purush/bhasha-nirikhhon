const timeDisplay = document.querySelector('.time')
const temp = document.querySelector('.temp')
const dateDisplay = document.querySelector('.date')
const nameDisplay = document.querySelector('.name')
const weatherIcon = document.querySelector('.weather-icon')
const conditionDisplay = document.querySelector('.condition')
const cloudDisplay = document.querySelector('.cloud')
const humidityDisplay = document.querySelector('.humidity')
const windDisplay = document.querySelector('.wind')
const search = document.querySelector('.search')
const form = document.getElementById('get-city')
const btn = document.querySelector('.submit')
const home = document.querySelector('.home')
const cities = document.querySelectorAll('.city')
const forecastCardsContainer = document.getElementById('forecast-cards')
const apiKey = 'e00ceb505c7c48ebbda92341222112'

let cityInput = 'London'

cities.forEach((city) => {
  city.addEventListener('click', (e) => {
    cityInput = e.target.innerHTML
    updateWeather()``
  })
})

form.addEventListener('submit', (e) => {
  e.preventDefault()
  if (search.value.trim()) {
    cityInput = search.value
    updateWeather()
    search.value = ''
  } else {
    alert('Please type a city name')
  }
})

function dayOfWeek(day, month, year) {
  const weekday = [
    'Sunday',
    'Monday',
    'Tuesday',
    'Wednesday',
    'Thursday',
    'Friday',
    'Saturday',
  ]
  return weekday[new Date(`${year}-${month}-${day}`).getDay()]
}

function updateWeather() {
  fetchWeatherData()
  fetchWeatherForecast()
  home.style.opacity = '0'
}

function fetchWeatherData() {
  fetch(
    `https://api.weatherapi.com/v1/current.json?key=${apiKey}&q=${cityInput}&aqi=no`
  )
    .then((response) => response.json())
    .then((data) => {
      const current = data.current
      const location = data.location

      temp.innerHTML = `${current.temp_c}&#176;`
      conditionDisplay.innerHTML = current.condition.text
      dateDisplay.innerHTML = `${dayOfWeek(
        location.localtime.substr(8, 2),
        location.localtime.substr(5, 2),
        location.localtime.substr(0, 4)
      )} ${location.localtime.substr(8, 2)}, ${location.localtime.substr(
        5,
        2
      )}, ${location.localtime.substr(0, 4)}`
      timeDisplay.innerHTML = location.localtime.substr(11)
      nameDisplay.innerHTML = location.name
      weatherIcon.src = current.condition.icon
      cloudDisplay.innerHTML = `${current.cloud}%`
      humidityDisplay.innerHTML = `${current.humidity}%`
      windDisplay.innerHTML = `${current.wind_kph} km/hr`

      let timeOfDay = current.is_day ? 'day' : 'night'
      let backgroundImage = `url(./icons/${timeOfDay}/`

      switch (current.condition.code) {
        case 1000:
          backgroundImage += 'clear.jpg'
          btn.style.background = timeOfDay === 'day' ? '#e5ba92' : '#181e27'
          break
        case 1003:
        case 1006:
        case 1009:
        case 1030:
        case 1069:
        case 1087:
        case 1135:
        case 1273:
        case 1276:
        case 1279:
        case 1282:
          backgroundImage += 'cloudy.jpg'
          btn.style.background = timeOfDay === 'day' ? '#fa6d1b' : '#181e27'
          break
        case 1063:
        case 1069:
        case 1072:
        case 1150:
        case 1153:
        case 1180:
        case 1183:
        case 1186:
        case 1189:
        case 1192:
        case 1195:
        case 1204:
        case 1207:
        case 1240:
        case 1243:
        case 1246:
        case 1249:
        case 1252:
          backgroundImage += 'rainy.jpg'
          btn.style.background = timeOfDay === 'day' ? '#647d75' : '#325c80'
          break
        default:
          backgroundImage += 'snowy.jpg'
          btn.style.background = timeOfDay === 'day' ? '#4d72aa' : '#1b1b1b'
          break
      }

      home.style.backgroundImage = backgroundImage
      home.style.opacity = '1'
    })
    .catch(() => {
      alert('City not found!!!')
      home.style.opacity = '1'
    })
}

function fetchWeatherForecast() {
  forecastCardsContainer.innerHTML = ''
  fetch(
    `https://api.weatherapi.com/v1/forecast.json?key=${apiKey}&q=${cityInput}&days=4&aqi=no`
  )
    .then((response) => response.json())
    .then((data) => {
      const forecastData = data.forecast.forecastday

      forecastData.forEach((day) => {
        const card = document.createElement('div')
        card.className = 'forecast-card'

        const date = new Date(day.date).toLocaleDateString('en-GB', {
          weekday: 'short',
          day: 'numeric',
          month: 'short',
        })

        card.innerHTML = `
                    <h3>${date}</h3>
                    <img src="${day.day.condition.icon}" alt="${day.day.condition.text}">
                    <p>${day.day.condition.text}</p>
                    <p>Max: ${day.day.maxtemp_c}°C</p>
                    <p>Min: ${day.day.mintemp_c}°C</p>
                `

        forecastCardsContainer.appendChild(card)
      })
    })
    .catch((error) => {
      console.error('Error fetching weather data:', error)
    })
}

updateWeather()