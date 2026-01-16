const cityName = document.getElementById("cityName");
const city_name = document.getElementById("city_name");

const submitBtn = document.getElementById("submitBtn");

const temp_status = document.getElementById("temp_status");
const temp_real_val = document.getElementById("temp_real_val");

const datahide = document.querySelector(".middle_layer");

const getInfo = async (event) => {
  event.preventDefault();
  let cityVal = cityName.value;
  if (cityVal === "") {
    city_name.innerText = `Plz writer the name of city before search`;
    datahide.classList.add("data_hide"); //using class
  } else {
    try {
      let url = `http://api.openweathermap.org/data/2.5/weather?q=${cityVal}&units=metric&appid=329f5ec042bf83f507cb8278ad19a182`;
      const response = await fetch(url);
      //   console.log(response);
      const data = await response.json(); //convert it into object data
      //   console.log(data);
      const arrData = [data]; //convert it into array of an object
      city_name.innerText = `${arrData[0].name}, ${arrData[0].sys.country}`;
      temp_real_val.innerText = arrData[0].main.temp;

      //condition to check temperature status
      const tempMood = arrData[0].weather[0].main;
      if (tempMood == "Clear") {
        temp_status.innerHTML =
          "<i class='fas fa-sun' style='color: #eccc68;'></i>";
      } else if (tempMood == "Clouds") {
        temp_status.innerHTML =
          "<i class='fas fa-cloud' style='color: #f1f2f6;'></i>";
      } else if (tempMood == "Rain") {
        temp_status.innerHTML =
          "<i class='fas fa-cloud-rain' style='color: #a4b0be;'></i>";
      } else {
        temp_status.innerHTML =
          "<i class='fas fa-sun' style='color:  #eccc68;'></i>";
      }
      datahide.classList.remove("data_hide"); //removing class
    } catch {
      city_name.innerText = `Plz enter the city name properly`;
      datahide.classList.add("data_hide"); //using class
    }
  }
};
submitBtn.addEventListener("click", getInfo);