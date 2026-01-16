import React from "react";

import Titles from "./components/Titles";
import Form from "./components/Form";
import Weather from "./components/Weather";

const API_KEY = "a5774b19c0bb713704ac27aad1764191";

class App extends React.Component {
  state = {
    temperature: undefined,
    city: undefined,
    country: undefined,
    humidity: undefined,
    description: undefined,
    error: undefined
  }
  getWeather = async (e) => {
    // var city, country
    e.preventDefault();
    const city = "GHAZIABAD";
    const country = "India";
    // const city = e.target.elements.city.value;
    // const country = e.target.elements.country.value;
    console.log("country>>>>", country)
    const api_call = await fetch(`http://api.openweathermap.org/data/2.5/weather?q=${city},${country}&appid=${API_KEY}&units=metric`);
    // const api_call = await fetch(`http://api.openweathermap.org/data/2.5/forecast?id=1271308&appid=${API_KEY}`);
    console.log("api_call>>>>", api_call)
    const data = await api_call.json();
    console.log("data>>>>", data)
        // city= data.name
    // var country= data.sys.country
    console.log("country>>>", country)
    if (data.sys.country === "IN") {
      this.setState({
        temperature: data.main.temp,
        city: data.name,
        country: data.sys.country,
        humidity: data.main.humidity,
        description: data.weather[0].description,
        error: ""
      });
    } else {
      this.setState({
        temperature: undefined,
        city: undefined,
        country: undefined,
        humidity: undefined,
        description: undefined,
        error: "Please enter the values."
      });
    }
  }
  render() {

    return (
      <div>
        <div className="wrapper">
          <div className="main">
            <div className="container">
              <div className="row">
                <div className="col-xs-5 title-container">
                  <Titles />
                </div>
                <div className="col-xs-7 form-container">
                  <Form getWeather={this.getWeather} />
                  {/* <button onClick={this.getWeather}>Click Me</button> */}
                  <Weather 
                    temperature={this.state.temperature} 
                    humidity={this.state.humidity}
                    city={this.state.city}
                    country={this.state.country}
                    description={this.state.description}
                    error={this.state.error}
                  />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
};

export default App;