const getTopSongsData = async () => {
  try {
    const songs = await getData(
      'https://qtify-backend-labs.crio.do/albums/top'
    );
    return songs;
  } catch (error) {
    console.log(error);
    return [];
  }
};
const getNewSongsData = async () => {
  try {
    const songs = await getData(
      'https://qtify-backend-labs.crio.do/albums/new'
    );
    return songs;
  } catch (error) {
    console.log(error);
    return [];
  }
};
const getSongsData = async () => {
  try {
    const songs = await getData('https://qtify-backend-labs.crio.do/songs');
    return songs;
  } catch (error) {
    console.log(error);
    return [];
  }
};

const getGenresData = async () => {
  try {
    const genres = await getData('https://qtify-backend-labs.crio.do/genres');
    return genres.data;
  } catch (error) {
    console.log(error);
    return [];
  }
};

const getData = async (url) => {
  try {
    const response = await fetch(url);
    const data = await response.json();
    return data;
  } catch (error) {
    console.error(error);
    return [];
  }
};

export { getTopSongsData, getNewSongsData, getSongsData, getGenresData };