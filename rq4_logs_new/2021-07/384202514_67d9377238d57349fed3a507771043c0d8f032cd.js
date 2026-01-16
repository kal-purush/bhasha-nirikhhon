import React, { useState } from "react";
import axios from "axios";

export default function Hello(props){
let [temperature, setTemperature]=useState(null)

function handleResponse(response){
    
    setTemperature (Math.round(response.data.main.temp));
}
let apiKey = "a2448133104335b630f878b5541b3167";
    let apiUrl = `https://api.openweathermap.org/data/2.5/weather?q=${props.city}&appid=${apiKey}&units=metric`;
    axios.get(apiUrl).then(handleResponse)
    
    return ( 
        <div>
        <h4>The temperature in {props.city} is {temperature}°C</h4>
        </div>
        

    )
}