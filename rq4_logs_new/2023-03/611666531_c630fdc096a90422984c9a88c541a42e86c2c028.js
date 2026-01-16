
async function getPeople(endpoint) {
    let data = await fetch(endpoint);
    let json = await data.json();
    return json;
}

class Character {
    constructor(name, gender, height, mass, hair_color, skinColor, eyeColor, movies, pictureUrl) {
        this.name = name;
        this.gender = gender;
        this.height = height;
        this.mass = mass;
        this.hair_color = hair_color;
        this.skinColor = skinColor;
        this.eyeColor = eyeColor;
        this.movies = [];
        this.pictureUrl = pictureUrl;
    }
};

let contentWrapper = document.querySelector('#contentWrapper');
let characterBtn = document.querySelector('#characterBtn');
let selectOne= document.querySelector('#selectOne');
let selectTwo= document.querySelector('#selectTwo');




const getCharacters = () => {

    characterBtn.addEventListener('click', async () => {
      contentWrapper.innerHTML = '';
      let characterOneInfo = await getPeople(`https://swapi.dev/api/people/${selectOne.value}`);
      let characterTwoInfo = await getPeople(`https://swapi.dev/api/people/${selectTwo.value}`);
      //funktionalitet för att hämta filmer
        let characterOneMovies = [];
        let characterTwoMovies = [];
        for (let i = 0; i < characterOneInfo.films.length; i++) {
            let movie = await getPeople(characterOneInfo.films[i]);
            characterOneMovies.push(movie);
        }
        for (let i = 0; i < characterTwoInfo.films.length; i++) {
            let movie = await getPeople(characterTwoInfo.films[i]);
            characterTwoMovies.push(movie);
        };

        //funktionalitet för att hämta filmer slutar här
        
        let characterOne = new Character(
            characterOneInfo.name,
            characterOneInfo.gender,
            characterOneInfo.height,
            characterOneInfo.mass,
            characterOneInfo.hair_color,
            characterOneInfo.skinColor,
            characterOneInfo.eyeColor,
            );
            let characterTwo = new Character(
                characterTwoInfo.name,
                characterTwoInfo.gender,
                characterTwoInfo.height,
                characterTwoInfo.mass,
                characterTwoInfo.hair_color,
                characterTwoInfo.skinColor,
                characterTwoInfo.eyeColor,
                );

                
      let characterDiv = document.createElement('div');
      characterDiv.innerHTML = `<div><p>${characterOne.name}</p>
      <div id="characterOneInfo"></div>
      </div><div>
      <p>${characterTwo.name}</p>
      <div id="characterTwoInfo"></div>
      </div>
      <button id="compareBtn">Compare</button>
     `;
      contentWrapper.append(characterDiv);

        let compareBtn = document.querySelector('#compareBtn');
        let compareInfoOne = document.querySelector('#characterOneInfo');
        let compareInfoTwo = document.querySelector("#characterTwoInfo");
        compareBtn.addEventListener('click', () => {
let characterOneInfoDiv= document.createElement('div');
characterOneInfoDiv.innerHTML = `
<div>
<li>Gender: ${characterOneInfo.gender}</li>
<li>Height: ${characterOneInfo.height}</li>
<li>Mass: ${characterOneInfo.mass}</li>
<li>Haircolor: ${characterOneInfo.hair_color}</li>
<li>Skincolor: ${characterOneInfo.skin_color}</li>
<li>EyeColor: ${characterOneInfo.eye_color}</li>
<ol id="movieOne">Films:</ol>
</div>`
compareInfoOne.append(characterOneInfoDiv);

let characterTwoInfoDiv= document.createElement('div');
characterTwoInfoDiv.innerHTML =
`<div>
<li>Gender: ${characterTwoInfo.gender}</li>
<li>Height: ${characterTwoInfo.height}</li>
<li>Mass: ${characterTwoInfo.mass}</li>
<li>Haircolor: ${characterTwoInfo.hair_color}</li>
<li>Skincolor: ${characterTwoInfo.skin_color}</li>
<li>EyeColor: ${characterTwoInfo.eye_color}</li>
<ol id="movieTwo">Films:</ol>
`;
compareInfoTwo.append(characterTwoInfoDiv);

        characterOneMovies.forEach(movie => {
            let movieListWrapperOne = document.querySelector("#movieOne");
            let movieListOne = document.createElement("li");
            movieListOne.innerHTML = `${movie.title}`;
          movieListWrapperOne.append(movieListOne);
        });
        
        characterTwoMovies.forEach(movie => {
            let movieListWrapperTwo = document.querySelector("#movieTwo");
            let movieListTwo = document.createElement("li");
            movieListTwo.innerHTML = `${movie.title}`;
            movieListWrapperTwo.append(movieListTwo);
        });
        });
    });
    };



    const loadPage = async () => {
        let data = await getPeople('https://swapi.dev/api/');
        getCharacters(data);
      };
      
      loadPage();