const livros = require("./livros.json");

for (let i = 0; i < livros.length; i++) {
    for(let item in livros[i]) {
        console.log(`${item}: ${livros[i][item]}`)
    };
};
