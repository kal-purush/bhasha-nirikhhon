export const countryCodeToCountryName = (code) => {
  const regionNamesInEnglish = new Intl.DisplayNames(["en"], {
    type: "region",
  });
  return regionNamesInEnglish.of(code);
};

// convert temperature based on temperatureType
export const calculatedTemperature = (type, data) => {
  const temp = type === "Fahrenheit" ? data : ((data - 32) / 1.8).toFixed(1);
  return temp;
};