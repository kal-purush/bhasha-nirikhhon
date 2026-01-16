const load = document.querySelector('button');
const text = document.querySelector('p');

load.addEventListener('click', () => {
    axios.get("https://gateway.marvel.com:443/v1/public/characters?name=Gamora&apikey=586f2bef68940a3b0fce2ba7230e2f36")
    .then((response) => {
        console.log(response.data.data.results);
        text.innerText = JSON.stringify(response.data.data.results[0]);
    })
    .catch((error) => {
        console.log(error);
    })
})