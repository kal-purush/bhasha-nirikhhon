function getJson() {
    let city = document.querySelector('#input-city').value;
    fetch(`http://api.openweathermap.org/data/2.5/weather?q=${city}&appid=c941c9ca854444abd337e4baa9e01cb5
    `)
    .then(response => response.json())
    .then(data => {
        console.log(data)
   // let selected_city = 
    let high_temp = data.main.temp_max
    let low_temp = data.main.temp_min
    let forecast = data.weather[0].description
    let display_temp = document.createElement('td')
    display_temp.innerHTML = high_temp
    display_temp.innerHTML = low_temp
    display_temp.innerHTML = forecast
// Im trying to test out one row before I fill the rest of my columns
// but I cant even get one row to populate data ..... :( 
    document.querySelector(`#row${i+1}`).append(display_temp)
    })
}




