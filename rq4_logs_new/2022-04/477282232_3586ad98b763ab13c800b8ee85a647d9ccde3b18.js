const searchbtn = document.getElementById("submit-val");
const cityname = document.getElementById("cityname");
const cityshow = document.getElementById("cityshow");
const temprature = document.getElementById("temp");
const temp_status = document.getElementById("temp-status");
const getinfo = async(e) => {
    e.preventDefault();
    let city = cityname.value;
    if (city === "") {
        cityshow.textContent = `plese enter the city in box`;
    } else {
        try {

            let url = `https://api.openweathermap.org/data/2.5/weather?q=${city}&appid=d404b1dddd71ebccfdd018a1a9c43efe`;
            const responce = await fetch(url);
            const data = await responce.json();
            const maindata = [data];
            temprature.innerHTML = `<p>${maindata[0].main.temp} <sup>o</sup>C</p>`;
            cityshow.textContent = `${maindata[0].name},${maindata[0].sys.country}`;
            const status = maindata[0].weather[0].main;
            if (status === "Clear") {
                temp_status.innerHTML = `<i class='fas fa-sun' style='font-size:48px;color:yellow'></i>`;
            } else if (status === "Clouds") {
                temp_status.innerHTML = `<i class='fas fa-cloud-sun' style='font-size:48px;color:white'></i>`;
            } else if (status === "rain") {
                temp_status.innerHTML = `<i class='fas fa-sun' style='font-size:48px;color:yellow'></i>`;
            } else {
                temp_status.innerHTML = `<i class='fas fa-sun' style='font-size:48px;color:yellow'></i>`;
            }
        } catch (err) {
            cityshow.textContent = `plese enter proper city name`;
        }
    }
}


searchbtn.addEventListener("click", getinfo);