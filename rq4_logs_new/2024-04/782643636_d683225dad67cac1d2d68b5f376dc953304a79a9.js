import axios from 'axios';
export async function getImages(search, currentPage) {
  const BASE_URL = 'https://pixabay.com';
  const END_POINT = '/api/';
  const url = `${BASE_URL}${END_POINT}`;
  const params = {
    key: '43209712-864cbe761aaf2bd904c3cc70a',
    q: search,
    image_type: 'photo',
    orientation: 'horizontal',
    safesearch: true,
    page: currentPage,
    per_page: 15,
  };
  const res = await axios.get(url, { params });
  return res.data;
}