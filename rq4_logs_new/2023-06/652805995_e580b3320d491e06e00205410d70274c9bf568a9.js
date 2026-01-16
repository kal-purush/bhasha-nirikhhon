import React, { useState } from "react";
import axios from "axios";

function App() {
  const [data, setData] = useState();
  const [location, setLocation] = useState("");

  const url = `https://api.openweathermap.org/data/2.5/weather?q=${location}&units=imperial&appid=0e3e254dc5fec0c9266070a7f7ffc77a`;

  const searchLocation = (event) => {
    if (event.key === "Enter") {
      axios(url).then((response) => {
        setData(response.data);
        console.log(response.data);
      });
      setLocation("");
    }
  };

  return (
    <div className="app">
      <div className="search">
        <input
          value={location}
          onChange={(event) => setLocation(event.target.value)}
          onKeyDown={searchLocation}
          placeholder="Enter Location"
          type="text"
        />
      </div>

      <div className="container">
        <div className="top">
          <div className="location">{data ? <p>{data.name}</p> : null}</div>
          <div className="temp">
            {data ? <h1>{data.main.temp.toFixed()} F </h1> : null}
          </div>
          <div className="description">
            {data ? <p>{data.weather[0].main}</p> : null}
          </div>
        </div>
        <div className="bottom">
          <div className="feels">
            {data ? <p className="bold"> {data.main.feels_like.toFixed()} </p> : null}
            <p>Feels Like</p>
          </div>
          <div className="humidity">
            {data ? <p className="bold"> {data.main.humidity} </p> : null}
            <p>Humidity</p>
          </div>
          <div className="wind">
            {data ? <p className="bold"> {data.wind.speed.toFixed()} </p> : null}
            <p>Wind Speed</p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;