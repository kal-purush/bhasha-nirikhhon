//GET API DATA - FILMS 

fetch('https://swapi.co/api/films/')
    .then(response => response.json())
    .then(imprimeJson);
           
function imprimeJson(json) {
    console.log(json.results);
    let cardTemplate = document.querySelector('#cardTop');
    
    for (const iterator of json.results) {
        cardTemplate.content.querySelector(".titulo").textContent = iterator.title;
        cardTemplate.content.querySelector(".resumo").textContent = iterator.opening_crawl;
        cardTemplate.content.querySelector(".id").textContent = iterator.episode_id;
        cardTemplate.content.querySelector(".release").textContent = iterator.release_date;

        let clone = document.importNode(cardTemplate.content, true);
        caixa.appendChild(clone);

    }
} 