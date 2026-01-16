function pkmnStart () {
    // console.log("This did a thing");
    //  let id = 
    //  console.log(id);


    //there are currently 1026 pokemon in the national dex. This will change later, but setting as a const for now
    const POKEMON_MAX = 1026;

    let pkmnNameH1 = document.querySelector("#pkmnName");
    let pkmnImg = document.querySelector("#pkmnPic");
    let pkmnTypes = document.querySelector("#pkmnTypes");
    let pkmnDexP = document.querySelector("#pkmnDex");

    const typeColourKey = {"colorKey": [
        {type:"normal", colorMain: "brown", colorSecond: "#efe696"},
        {type:"bug", colorMain: "#6ee794", colorSecond: "#55453f" },
        {type: "flying", colorMain:"#6687b1", colorSecond: "#d9eb6b"},
        {type:"grass" , colorMain: "#0e550e", colorSecond: "#a2e158"},
        {type: "water", colorMain: "blue", colorSecond: "aqua"},
        {type:"fire" , colorMain: "red", colorSecond: "orange"},
        {type:"electric" , colorMain: "#656510", colorSecond: "#e5f356"},
        {type:"fairy" , colorMain: "#ef3c96", colorSecond: "pink"},
        {type: "ghost", colorMain: "#9c46c7", colorSecond: "#becddb"},
        {type:"dark" , colorMain: "grey", colorSecond: "black"},
        {type:"dragon", colorMain: "#05931f", colorSecond: "#12128b"},
        {type:"steel", colorMain:"#615d3e", colorSecond: "silver"},
        {type:"poison", colorMain: "#310d30", colorSecond: "#507525"},
        {type:"rock", colorMain : "brown", colorSecond: "grey"},
        {type:"ground", colorMain:"#3c4b53", colorSecond:"#e18c43"},
        {type:"psychic", colorMain: "#830832", colorSecond: "#eb9a4a"},
        {type:"ice", colorMain : "#54c5bb", colorSecond: "white"},
        {type:"fighting", colorMain:"#bf451f", colorSecond: "#79140c"}
    ]
}

    
    
    async function getPokemon(pkmnId) {
        console.log(typeColourKey);
        pkmnTypes.innerHTML="";

        pokemonid = pkmnId;

        let test = "test"
        
       

        if (pokemonid) {
            const response = await fetch("https://pokeapi.co/api/v2/pokemon/" + pokemonid);
            let pokemon = await response.json();
            // console.log(pokemon);

            const response2 = await fetch("https://pokeapi.co/api/v2/pokemon-species/" + pokemonid);
            let pokemonMore = await response2.json();
            console.log(pokemonMore);

            let pkmnName = capitalizeWord(pokemon.name);
            
            pkmnNameH1.innerText = `${pokemon.id}: ${pkmnName}`;
            pkmnImg.setAttribute('src', pokemon.sprites.front_default);

            // if (pokemon.types.length === 1) {
            //     pkmnTypes.innerText = pokemon.types[0].type.name;
            // }

            // if (pokemon.types.length === 2) {
            //     pkmnTypes.innerText = `${pokemon.types[0].type.name} / ${pokemon.types[1].type.name}`;
            // }

            pokemon.types.forEach(type => {
                
                for (let counter=0; counter < typeColourKey.colorKey.length; counter++ ) {
                    
                    if (type.type.name === typeColourKey.colorKey[counter].type) {
                        console.log(typeColourKey.colorKey[counter].type);
                        console.log(type.type.name);
                        let typeLi = document.createElement('li');
                        typeLi.innerText = type.type.name;
                        typeLi.setAttribute("style", `color:${typeColourKey.colorKey[counter].colorMain}; 
                        border: 2px solid ${typeColourKey.colorKey[counter].colorMain}; 
                        background-color:${typeColourKey.colorKey[counter].colorSecond}`);
                        pkmnTypes.appendChild(typeLi);
                        break;
                    }
                }
                
            })



            // console.log(pkmnDexP);

            let dexEntries = [];

            pokemonMore.flavor_text_entries.forEach (entry => {
                if (entry.language.name == "en") {
                    dexEntries.push(entry);
                }
            })



            newRandom = Math.floor(Math.random() * dexEntries.length);

            pkmnDexP.innerText = `${dexEntries[newRandom].flavor_text} (${dexEntries[newRandom].version.name})`;



        }
        else {
            console.log("No data");

        }     

    }

    function capitalizeWord(word) {
        //gets the character at the given position, so 0 is the first letter
        firstLetter = word.charAt(0).toUpperCase();
        //slice cuts the word at the given position (1 in this case)
        newWord = `${firstLetter}${word.slice(1)}`
        return newWord;
        
    }

    function getRandomPokemon() {

        let pkmnMax = POKEMON_MAX + 1;

        newRandomNumber = Math.floor(Math.random() * pkmnMax) + 1;
        // console.log (newRandomNumber);

        getPokemon(newRandomNumber);

    }

    function getPokemonFromUser() {

        let pokemonEntry = document.querySelector("#pkmnInput").value;

        if (pokemonEntry) {
            getPokemon(pokemonEntry)
        }
        else {
            getRandomPokemon();
        }
    }

    let submitButton= document.querySelector("#pkmnSubmit");
    submitButton.addEventListener("click", () => {
        getPokemonFromUser()
    });

    let randomButton= document.querySelector("#pkmnRandom");
    randomButton.addEventListener("click", () => {
        getRandomPokemon();
    });


    
    // getPokemon(id);

}

window.addEventListener('load', pkmnStart);