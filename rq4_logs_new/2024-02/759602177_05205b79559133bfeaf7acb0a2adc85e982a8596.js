const apiUrl = "https://v2.jokeapi.dev/joke/Programming?blacklistFlags=nsfw,religious,political,racist,sexist,explicit&type=twopart"

const fetchJoke = () => {
  return fetch(apiUrl, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
    .then(response => {
      return response.json();
    })
};


// fetchJoke().then(data => {
//   console.log(data)
// })

export default fetchJoke; 