const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});
var Request = require("request");

console.log("1. al-jazeera-english\n2. bbc-news\n3. cnn\n4. abc-news\n");
//an object showing news sources
const choosesource= () => {

    const news_sources = {
        1: "al-jazeera-english",
        2: "bbc-news",
        3: "cnn",
        4: "abc-news"
    };
    if (choice == 1) {

        Request.get("https://newsapi.org/v2/top-headlines?sources=bbc-news&apiKey= ec7e709fb8c64de7b26f268b2d159793", (error, response, body) => {
            if (error) {
                return console.dir(error);
            }
            console.dir(JSON.parse(body));
        });
    } else if (choice == 2) {

        Request.get("https://newsapi.org/v2/top-headlines?sources=fox-news&apiKey= ec7e709fb8c64de7b26f268b2d159793", (error, response, body) => {
            if (error) {
                return console.dir(error);
            }
            console.dir(JSON.parse(body));
        });
    } else if (choice == 3) {
        Request.get("https://newsapi.org/v2/top-headlines?sources=al-jazeera-english&apiKey= ec7e709fb8c64de7b26f268b2d159793", (error, response, body) => {
            if (error) {
                return console.dir(error);
            }
            console.dir(JSON.parse(body));
        });
    } else if (choice == 4) {
        Request.get("https://newsapi.org/v2/top-headlines?sources=cnn&apiKey= ec7e709fb8c64de7b26f268b2d159793", (error, response, body) => {
            if (error) {
                return console.dir(error);
            }
            console.dir(JSON.parse(body));
        });
    }

};
choosesource();
