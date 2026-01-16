import React, { useEffect, useState } from 'react';
import Jumbotron from "react-bootstrap/Jumbotron";
import Container from "react-bootstrap/Container";
import ToggleButton from 'react-bootstrap/ToggleButton';
import ToggleButtonGroup from 'react-bootstrap/ToggleButtonGroup';
import Search from './search/textbox.js';
import './App.css';

function App() {

  const [temp, setTemp] = useState({});
  const [error, setError] = useState(false);
  const [value, setValue] = useState("C");
  useEffect(() => {
    let cachedLat = localStorage.getItem("lat");
    let cachedLong = localStorage.getItem("long");
    if (cachedLat && cachedLong) {
      getWeatherDetails(cachedLat, cachedLong);
    }
    else {
      if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition((position) => {

          localStorage.setItem("lat", position.coords.latitude);
          localStorage.setItem("long", position.coords.longitude);
          getWeatherDetails(position.coords.latitude, position.coords.longitude, null);
        }, (error) => {
          setError(true)
        })
      }
      else {
        setError(true);
      }
    }

  }, []);
  function getWeatherDetails(lat, long, city) {
    let url = '';
    console.log(lat, long, city)
    if (lat != null) {
      url = `http://api.openweathermap.org/data/2.5/weather?&lat=${lat}&lon=${long}&units=metric&APPID=293bc4ee167ab628a8609a2a277b312e`
    }
    else {
      url = `http://api.openweathermap.org/data/2.5/weather?&q=${city}&units=metric&APPID=293bc4ee167ab628a8609a2a277b312e`
    }
    fetch(url).then((response) => {
      response.json().then((res) => {
        console.log(res)
        if (res.cod === 200) {
          setTemp({
            temp: res.main.temp,
            place: res.name,
            description: res.weather[0].description
          })
        }else{
          setError(true);
        }

      }).catch((err) => {
        setError(true);
      })
    })

  }

  const handleChange = ((val) => {
    let temperature = temp.temp;
    if (val === "F") {
      temperature = temperature * 9 / 5 + 32;
    }
    if (val === "C") {
      temperature = ((5 / 9) * (temperature - 32)).toFixed(2);
    }
    setTemp({
      ...temp,
      temp: temperature
    });
    setValue(val);
  })

  const citySearch = (city) => {
    getWeatherDetails(null, null, city);
  }

  return (
    <div className="App">
      <title>Weather Info</title>

      <Container className="p-3">
        <Jumbotron>
          <Search searchCity={citySearch}></Search><br /><br />
          <h4 className="header">{temp.place}</h4>
          <span>
            <h1> {temp.temp}<span>&deg;{value}</span> </h1>
            <ToggleButtonGroup type="radio" name="Farenheit" value={value} onChange={handleChange}>
              <ToggleButton value={"F"}>Farenheit</ToggleButton>
              <ToggleButton value={"C"}>Celcius</ToggleButton>
            </ToggleButtonGroup>
            <br />
            <h5>{temp.description}</h5><br />
          </span>
        </Jumbotron>

      </Container>

      {error && <div style={{ color: `red` }}>Error occurred while fetching the coordinates</div>}
    </div>
  );
}

export default App;