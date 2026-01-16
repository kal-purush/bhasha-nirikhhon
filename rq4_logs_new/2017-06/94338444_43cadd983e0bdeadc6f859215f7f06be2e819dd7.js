import React from 'react';
import ReactDOM from 'react-dom'
import DatePicker from 'react-datepicker';
import axios from 'axios';
import 'react-datepicker/dist/react-datepicker.css';

class Weather extends React.Component {
  constructor() {
    super();
    this.state = {
      city: ''
    };
    this.handleChange = this.handleChange.bind(this);

  }
  findLatLongByCityName(city){
    //http://maps.googleapis.com/maps/api/geocode/json?address=Miami+FL&sensor=false
    console.log(city);
    axios.get('http://maps.googleapis.com/maps/api/geocode/json?address='+ this.state.city +'&sensor=false')
      .then(res => {
        //const posts = res.data.data.children.map(obj => obj.data);
        //console.log(res.data.results["0"].geometry.location);
        var lat = res.data.results["0"].geometry.location.lat;
        var lon = res.data.results["0"].geometry.location.lng;
        var time = '1497549600'
        var proxy = 'https://cors-anywhere.herokuapp.com/';
        var url = proxy + 'https://api.darksky.net/forecast/1d91e20fc6218bafefe2b9bd74b2df12/'+lat+','+lon+','+time
        //https://api.darksky.net/forecast/[key]/[latitude],[longitude]
        return axios.get(url)
          .then(res =>{
            console.log(res);
          });

    });
    return null;
  }
  handleChange({ target }) {
    this.setState({
      [target.name]: target.value
    });
  }

  render() {
    return (
      <div>
        <input
          name="city"
          onChange={this.handleChange}
          value={ this.state.city }
          type='text'
          placeholder='Enter a city'
        />
        <DatePicker
          selected={this.state.startDate}
          onChange={this.handleChange}
        />
        <button onClick={() => this.findLatLongByCityName(this.state.city)}>See Forecast</button>
      </div>
    );
  }
}
ReactDOM.render(
  <Weather />,
  document.getElementById('root')
);