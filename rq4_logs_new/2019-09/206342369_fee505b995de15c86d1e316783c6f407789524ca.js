import React from 'react';
import { CardContent, Box, Typography } from '@material-ui/core';

export default function ({ cityWeather }) {

  return (
    <CardContent>
      {
        cityWeather.weather.map((weatherItem) => (
          <Box key={weatherItem.id} display='flex' alignItems='center'>
            <img src={weatherItem.icon} alt="" />
            {weatherItem.description}
          </Box>
        ))
      }
      <Typography component='p'>
        Wind speed: {cityWeather.wind.speed} m/s
          </Typography>
      <Typography component='p'>
        Temperature: {cityWeather.main.temp} &deg;C
          </Typography>
      <Typography component='p'>
        Temperature Min: {cityWeather.main.temp_min} &deg;C
          </Typography>
      <Typography component='p'>
        Temperature Max: {cityWeather.main.temp_max} &deg;C
          </Typography>
      <Typography component='p'>
        Pressure: {cityWeather.main.pressure} &deg;C
          </Typography>
      <Typography component='p'>
        Humidity: {cityWeather.main.humidity}
      </Typography>
    </CardContent>
  )
}