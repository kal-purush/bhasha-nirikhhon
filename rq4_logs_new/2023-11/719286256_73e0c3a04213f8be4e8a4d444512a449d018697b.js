import axios from 'axios';

axios.defaults.headers.common['x-api-key'] =
  'live_KZNqPaZLIqp6ZlhsPgRMapnL4XgHVPp9ogre63NkeXfVrj13nob00xbvpsdPBwU7';
  
  const BASE_URL = 'https://api.thecatapi.com/v1';

  export function fetchBreeds() {
    const END_POINT = '/breeds';
    const PARAMS = new URLSearchParams('');
    const url = `${BASE_URL}${END_POINT}${PARAMS}`;
  
    return axios
      .get(url)
      .then(response => {
        if (response.status === 200) return response;
        return Promise.reject(response.status);
      })
      .catch(error => {
        errorFn(error);
      })
      .finally(loadOn());
  }
  
  export function fetchCatByBreed(breedId) {
    const END_POINT = '/images/search';
    const PARAMS = new URLSearchParams({ breed_ids: breedId });
    const url = `${BASE_URL}${END_POINT}?${PARAMS}`;
  
    return axios
      .get(url)
      .then(response => {
        if (response.status === 200) return response;
        return Promise.reject(response.status);
      })
      .catch(error => {
        errorFn(error);
      })
      .finally(loadOn());
  }