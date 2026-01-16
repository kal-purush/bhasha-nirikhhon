let story = 'Érase una vez una niñita que lucía una hermosa capa \
de color rojo. Como la niña la usaba muy a menudo, todos la \
llamaban Caperucita Roja. Un día, la mamá de Caperucita Roja la \
llamó y le dijo: —Abuelita no se siente muy bien, he horneado \
unas galletitas y quiero que tú se las lleves. —Claro que sí \
—respondió Caperucita Roja, poniéndose su capa y llenando su \
canasta de galletas recién horneadas. Antes de salir, su mamá \
le dijo: — Escúchame muy bien, quédate en el camino y nunca \
hables con extraños. —Yo sé mamá —respondió Caperucita Roja y \
salió inmediatamente hacia la casa de la abuelita';

// separa las palabras y caracteres (,.— ) en array
let storyWords = story.toLowerCase().split(/[\s,.— ]+/);

let unnecessaryCharacters = ['—', '.', ','];   

//se obtienen solo las palabras excluyendo las no necesarias
const betterWords = storyWords.filter( (word) => { 
    return !unnecessaryCharacters.includes(word);
});

const wordsToJson = (str) => { 
    return str.reduce( (count, word) => {
      count[word] = count.hasOwnProperty(word) ? count[word] + 1 : 1;      
      return count;
    }, {});
}

console.log(JSON.stringify(wordsToJson(betterWords)));