function callApi() {
  fetch("https://api.punkapi.com/v2/beers")
    .then((response) => {
      return response.json();
    })
    .then((data) => {
      const html = data
        .map((beer) => {
          console.log(beer.name);
          return `<div class="beer" id=${beer.id}>
          <div class="beer__img">
            <img
              src=${beer.image_url}
              alt="beer"
                width="100"
            />
            <h3>${beer.tagline}</h3>
            
          </div>
          <div class="beer__details">
            
            <h3>Name: ${beer.name}</h3>
            
            <p>${beer.description.substring(1, 50)}...</p>
            <p>First Brewed - ${beer.first_brewed}</p>
            <p>Brewer tips: ${beer.brewers_tips}</p>
            <p>By - ${beer.contributed_by}</p>
          </div>
        </div>`;
        })
        .join();
      document.querySelector("#app").insertAdjacentHTML("afterbegin", html);
    })
    .catch((error) => {
      console.log(error);
    });
}