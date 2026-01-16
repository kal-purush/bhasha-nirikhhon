const loadCountry = () => {
   fetch('https://restcountries.com/v3.1/all')
      .then(res => res.json())
      .then(data => displayCountry(data))
}

const displayCountry = country => {
   console.log(country);
}






loadCountry();