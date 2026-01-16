import React from "react";
import { useState } from "react";
const Timer = () => {
  const [minutes, setMinutes] = useState(0);
  const [seconds, setSeconds] = useState(0);
  const deadline = "December, 31, 2022";
  const getTime = () => {
    const time = Date.parse(deadline) - Date.now();
  };
  return <div className="timer"></div>;
};

export default Timer;