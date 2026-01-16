// import { error } from '../notify/notification';
function fetchCountries(searchQuery) {
  const url = `https://restcountries.com/v2/name/${searchQuery}`;

  // return fetch(url).then(res => res.json());
  return fetch(url)
    .then(res => (res.ok ? res.json() : []))
    .catch(error => {
      error({ text: 'Network error!' });
      console.log(error);
    });
}
export default fetchCountries;
