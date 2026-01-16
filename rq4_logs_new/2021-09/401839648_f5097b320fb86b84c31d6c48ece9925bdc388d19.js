import { DateTime } from "luxon";

import "./CityTemp.css";

const CityTemp = (props) => {
  const temperatureUnit =
    props.temperature.length === 0 ? "" : props.temperature[0].temperatureUnit;
  const currentTemperature =
    props.temperature.length === 0 ? "" : props.temperature[0].temperature;
  return (
    <div>
      <ul>
        <li>
          City:{" "}
          {props.info === null
            ? "loading"
            : props.info.properties.relativeLocation.properties.city}
        </li>
        <li>
          State:{" "}
          {props.info === null
            ? "loading"
            : props.info.properties.relativeLocation.properties.state}
        </li>
        <li>
          Temperature: {currentTemperature}
          {temperatureUnit === "F" ? "F" : "C"}
        </li>
        <li>
          Temperature:{" "}
          {temperatureUnit === "F"
            ? ((currentTemperature - 32) * (5 / 9)).toFixed(2) + "C"
            : (currentTemperature * (9 / 5) + 32).toFixed(2) + "F"}
        </li>
        <li>
          Time Zone:{" "}
          {props.info === null ? "loading" : props.info.properties.timeZone}
        </li>
        <li>
          Time:{" "}
          {props.info === null
            ? "loading"
            : DateTime.now()
                .setZone(props.info.properties.timeZone)
                .toLocaleString(DateTime.TIME_SIMPLE)}
        </li>
      </ul>
    </div>
  );
};

export default CityTemp;