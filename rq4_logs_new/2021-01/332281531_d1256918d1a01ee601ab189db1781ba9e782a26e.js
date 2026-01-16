import { useState,useEffect } from "react"; 
import {
  MenuItem,
  FormControl,
  Select,
  Card,
  CardContent
} from "@material-ui/core";
import InfoBox from './infoBox';
import Map from './map';
import Table from './table';
import Linegraph from './Linegraph';
import { sortData } from './util';
import './App.css';

function App() {
  const [countries, setCountries] = useState([]);
  const [country, setCountry] = useState('worldwide');
  const [countryInfo, setcountryInfo] = useState([]);
  const [tableData, settableData] = useState([]);

  useEffect(() => {
    fetch('https://disease.sh/v3/covid-19/all')
    .then(res => res.json())
    .then(data => {
      setcountryInfo(data);
    });
  },[]);

  useEffect(() => {
    const getCountriesData = async() => {
      await fetch("https://disease.sh/v3/covid-19/countries")
      .then((res) => res.json())
      .then((data) => { 
        const countries = data.map(country => (
          {
            name : country.country,
            value : country.countryInfo.iso2
          }
        ));

        const sortedData = sortData(data); 
        settableData(sortedData);
        setCountries(countries);
      });
    };

    getCountriesData();
  }, []);

  const onCountryChange = async(event) =>{
    const countryCode = event.target.value;
    
    const url = (countryCode === 'worldwide') ? 'https://disease.sh/v3/covid-19/all' : `https://disease.sh/v3/covid-19/countries/${countryCode}`;

    await fetch(url)
    .then(res => res.json())
    .then(data => {
      setCountry(countryCode);
      setcountryInfo(data);
    });
  }

  return (
    <div className="app">
      <div className="app__left">
        <div className="app__header">
          <h1>Covid tracker</h1>
          <FormControl className="app__dropdown">
            <Select
              variant="outlined"
              value={country}
              onChange={onCountryChange}
            >
              <MenuItem value="worldwide">Worldwide</MenuItem>
              {
                countries.map((country) => {
                  return <MenuItem value={country.value}>{country.name}</MenuItem>
                })
              }
            </Select>
          </FormControl>
        </div>

        <div className="app__stats">
              <InfoBox title="Coronavirus Cases" cases={countryInfo.todayCases} total={countryInfo.cases}/>
              <InfoBox title="Recovered" cases={countryInfo.todayRecovered} total={countryInfo.recovered}/>
              <InfoBox title="Deaths" cases={countryInfo.todayDeaths} total={countryInfo.deaths}/>
        </div>      

        <Map />
      </div>

      <Card className="app__right">
          <CardContent>
            <h3>Live cases by Country</h3>
            <Table countries={tableData} />
            <h3>Worldwide new Cases</h3>
            <Linegraph />
          </CardContent>
      </Card>        

            
    </div>
  );
}

export default App;