import axios from 'axios';
API_KEY = '30657377-211d44024b04d0f4a26eedce4';
BASE_URL = 'https://pixabay.com/api/';

export async function getImages(imageToSearch, currentPage) {
  const image = await axios.get(
    `${BASE_URL}?key=${API_KEY}&q=${imageToSearch}&image_type=photo&orientation=horizontal&safesearch=true&per_page=40&page={currentPage}`
  );
  return image.data;
}