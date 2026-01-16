import React from "react";
import WeatherIcon from "./WeatherIcon";

function ForecastDay(props) {
  function weekday() {
    let date = new Date(props.data.dt * 1000);
    let day = date.getDay();

    let days = [
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
    ];

    return days[day];
  }

  return (
    <li className="ForecastDay">
      <WeatherIcon code={props.data.weather[0].icon} size={40} />
      <div>
        {" "}
        <strong>{weekday()}</strong>
      </div>
      <div>
        {Math.round(props.data.temp.max)}˚ | {Math.round(props.data.temp.min)}˚
      </div>
    </li>
  );
}

export default ForecastDay;