export function handleTempChange(isCelsius: boolean, cityName: any) {
  if (cityName && cityName.length && isCelsius === true) {
    for (let i = 0; i < cityName.length; i++) {
      cityName[i].temp = Math.round((cityName[i].temp * 9) / 5 + 32);
      cityName[i].maxTemp = Math.round((cityName[i].maxTemp * 9) / 5 + 32);
      cityName[i].minTemp = Math.round((cityName[i].minTemp * 9) / 5 + 32);
    }
    return cityName;
  }

  if (cityName && cityName.length && isCelsius === false) {
    for (let i = 0; i < cityName.length; i++) {
      cityName[i].temp = Math.round(((cityName[i].temp - 32) * 5) / 9);
      cityName[i].maxTemp = Math.round(((cityName[i].maxTemp - 32) * 5) / 9);
      cityName[i].minTemp = Math.round(((cityName[i].minTemp - 32) * 5) / 9);
    }
    return cityName;
  }
}