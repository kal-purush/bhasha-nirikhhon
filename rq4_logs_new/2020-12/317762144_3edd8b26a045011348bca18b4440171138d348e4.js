import React, { useRef, useState } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { getLocation } from '../redux/action/WeatherActions';
import AsideWeatherScreen from './AsideWeatherScreen';
import Loader from './Loader';

const AsideWeather = ({
  weatherAbbr,
  weatherName,
  currTemp,
  day,
  date,
  month,
  locationName,
  getCurrWeather,
  asideMain,
  asideSearch,
  searchResult,
  changeDeg,
  errorWeather,
  errorText,
}) => {
  const dispatch = useDispatch();

  const [inputValue, setInputValue] = useState('');

  const { loading } = useSelector((state) => state.userWeather);

  // const asideMain = useRef(null);
  // const asideSearch = useRef(null);
  const searchText = useRef(null);
  const inputValueText = useRef(null);
  // const searchResult = useRef(null);

  const asideSearchBtn = (e) => {
    setTimeout(() => {
      asideMain.current.style.display = 'none';
      asideSearch.current.style.display = 'block';
    }, 300);
  };

  const asideSearchClose = () => {
    setTimeout(() => {
      asideSearch.current.style.display = 'none';
      asideMain.current.style.display = 'block';
      errorText.current.style.display = 'none';
      // searchText.current.textContent = '';
      // searchResult.current.style.display = 'none';
    }, 300);
  };

  const searchBtnHandler = () => {
    searchText.current.textContent = inputValue;
    searchResult.current.style.display = 'block';
    inputValueText.current.value = '';
    setInputValue('');
    dispatch(getLocation(inputValue));

    // console.log(location);
  };

  return (
    <>
      <aside className='Aside'>
        <div className='AsideMain' ref={asideMain}>
          <nav className='AsideNav'>
            <button className='BtnSearch' onClick={asideSearchBtn}>
              Search for places
            </button>
            <i className='material-icons GpsIcon'>gps_fixed</i>
          </nav>

          {/* <div className='WeatherScreen'>
            <div className='WeatherImg'>
              <img
                src={`https://www.metaweather.com/static/img/weather/${weatherAbbr}.svg`}
                alt='weather'
              />
            </div>

            <h1>
              {currTemp}
              <span className='AsideDegree'>&deg;C</span>
            </h1>

            <p className='WeatherType'>{weatherName}</p>

            <p className='Today'>
              Today . {day}, {date} {month}
            </p>

            <div className='Location'>
              <i className='material-icons LocationIcon'>location_on</i>
              <p className='LocationPlace'>{locationName}</p>
            </div>
          </div> */}

          {loading ? (
            <div className='LoaderAside'>
              <Loader />
            </div>
          ) : (
            <AsideWeatherScreen
              weatherAbbr={weatherAbbr}
              weatherName={weatherName}
              currTemp={currTemp}
              day={day}
              date={date}
              month={month}
              locationName={locationName}
              changeDeg={changeDeg}
              loading={loading}
            />
          )}
          {/* <Loader /> */}
        </div>

        <div className='AsideSearch' ref={asideSearch}>
          <i className='material-icons Clear' onClick={asideSearchClose}>
            clear
          </i>

          <div className='InputGrouping'>
            <i className='material-icons SearchIcon'>search</i>
            <div className='InputGroup'>
              <input
                type='text'
                placeholder='search location'
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                ref={inputValueText}
              />
              <button onClick={searchBtnHandler} className='SearchBtn'>
                Search
              </button>
            </div>
          </div>

          <div className='SearchResult' ref={searchResult}>
            <button className='SearchResultBtn' onClick={getCurrWeather}>
              <p className='SearchText' ref={searchText}></p>
              <i className='material-icons ArrowRight'>keyboard_arrow_right</i>
            </button>
          </div>

          <div className='ErrorWeather' ref={errorText}>
            <p className='ErrorText'>{errorWeather}</p>
          </div>
        </div>
      </aside>
    </>
  );
};

export default AsideWeather;