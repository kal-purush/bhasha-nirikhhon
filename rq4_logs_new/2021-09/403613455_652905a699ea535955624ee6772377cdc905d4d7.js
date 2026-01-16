const fetchCountries = searchQuery => {
  const URL_BASE = 'https://restcountries.eu/rest/v2/name';
  return (
    fetch(`${URL_BASE}/${searchQuery}`)
      //   return fetch('https:restcountries.eu/rest/v2/name/eesti')
      .then(res => res.json())
      .then(data => {
        console.log(data);
        if (data.status > 400) {
          return Promise.reject('Not found');
        }
        return data;
      })
      .catch(err => console.log(err))
  );
};
export default fetchCountries;