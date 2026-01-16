import React from "react";

export default function WeatherForecastDay(props){
    
    function day() {
        let date = new Date(props.data.time * 1000);
        let day = date.getDay();
    
        let days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    
        return days[day];
      }
    return(
       
          <div className="forecastDay">
            <div className="WeatherForecast-day">{day()}</div>
          <div>
            <img
              src={props.data.condition.icon_url}
              alt={props.data.condition.icon}
              className="float-left"
            />
          </div>
          <div className="forecastTemperature">
            <span className="forecastTemperatureMax">
              {Math.round(props.data.temperature.maximum)}
            </span>
            <span className="forecastTemperatureMin">
              {Math.round(props.data.temperature.minimum)}
            </span>
          </div>
        </div>);
}