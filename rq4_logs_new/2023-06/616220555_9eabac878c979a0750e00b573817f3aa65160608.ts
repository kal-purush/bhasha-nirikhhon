const tvGenres = [
    {
      "id": 10759,
      "name": "Action & Adventure"
    },
    {
      "id": 16,
      "name": "Animation"
    },
    {
      "id": 35,
      "name": "Comedy"
    },
    {
      "id": 80,
      "name": "Crime"
    },
    {
      "id": 99,
      "name": "Documentary"
    },
    {
      "id": 18,
      "name": "Drama"
    },
    {
      "id": 10751,
      "name": "Family"
    },
    {
      "id": 10762,
      "name": "Kids"
    },
    {
      "id": 9648,
      "name": "Mystery"
    },
    {
      "id": 10763,
      "name": "News"
    },
    {
      "id": 10764,
      "name": "Reality"
    },
    {
      "id": 10765,
      "name": "Sci-Fi & Fantasy"
    },
    {
      "id": 10766,
      "name": "Soap"
    },
    {
      "id": 10767,
      "name": "Talk"
    },
    {
      "id": 10768,
      "name": "War & Politics"
    },
    {
      "id": 37,
      "name": "Western"
    }
  ]

const movieGenres = [
    {
      "id": 28,
      "name": "Action"
    },
    {
      "id": 12,
      "name": "Adventure"
    },
    {
      "id": 16,
      "name": "Animation"
    },
    {
      "id": 35,
      "name": "Comedy"
    },
    {
      "id": 80,
      "name": "Crime"
    },
    {
      "id": 99,
      "name": "Documentary"
    },
    {
      "id": 18,
      "name": "Drama"
    },
    {
      "id": 10751,
      "name": "Family"
    },
    {
      "id": 14,
      "name": "Fantasy"
    },
    {
      "id": 36,
      "name": "History"
    },
    {
      "id": 27,
      "name": "Horror"
    },
    {
      "id": 10402,
      "name": "Music"
    },
    {
      "id": 9648,
      "name": "Mystery"
    },
    {
      "id": 10749,
      "name": "Romance"
    },
    {
      "id": 878,
      "name": "Science Fiction"
    },
    {
      "id": 10770,
      "name": "TV Movie"
    },
    {
      "id": 53,
      "name": "Thriller"
    },
    {
      "id": 10752,
      "name": "War"
    },
    {
      "id": 37,
      "name": "Western"
    }
  ]

const personGenders = [
  {
    "id": 0,
    "name": "Others"
  },
  {
    "id": 1,
    "name": "Female"
  },
  {
    "id": 2,
    "name": "Male"
  },
]

const multiGenres = tvGenres.concat(movieGenres, personGenders);

export const getTvGenreId = (genreName: string): number => {
  return tvGenres.filter((genre) => genre.name === genreName)[0].id;
}

export const getMovieGenreId = (genreName: string): number => {
  return movieGenres.filter((genre) => genre.name === genreName)[0].id;
}

export const getPersonGenderId = (genderName: string): number => {
  return personGenders.filter((gender) => gender.name === genderName)[0].id;
}

export const getMultiGenreId = (genreName: string): number => {
  return multiGenres.filter((genre) => genre.name === genreName)[0].id;
}

export const tvGenresNames = tvGenres.map((genre) => genre.name);

export const movieGenresNames = movieGenres.map((genre) => genre.name);

export const personGendersNames = personGenders.map((gender) => gender.name);

export const multiGenresNames = multiGenres.map((genre) => genre.name);