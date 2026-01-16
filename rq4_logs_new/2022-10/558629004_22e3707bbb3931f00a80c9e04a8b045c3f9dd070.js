import React, { useState } from "react";
import "./Temperature.css";

export default function Temperature(props) {
  const [unit, setUnit] = useState("celsius");

  function convertToFar(event) {
    event.preventDefault();
    setUnit("fahrenheit");
  }
  function convertToCel(event) {
    event.preventDefault();
    setUnit("celsius");
  }

  function fahrenheit() {
    return (props.celsius * 9) / 5 + 32;
  }
  if (unit === "celsius") {
    return (
      <div className="temperature">
        <span className="temp">{props.celsius}</span>
        <span className="units">
          °C |{" "}
          <a href="/" onClick={convertToFar}>
            °F
          </a>
        </span>
      </div>
    );
  } else {
    return (
      <div className="temperature">
        <span className="temp">{Math.round(fahrenheit())}</span>
        <span className="units">
          <a href="/" onClick={convertToCel}>
            °C
          </a>{" "}
          | °F
        </span>
      </div>
    );
  }
}