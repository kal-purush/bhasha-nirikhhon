'use strict';

const app = document.getElementById('root');

const logo = document.createElement('img');
logo.src= './img/logo.png';

const container = document.createElement('div');
container.setAttribute('class', 'container');

const footer = document.createElement('footer');
const copy = document.createElement('p');
const footerP = document.createTextNode('Ⓒ Copyright 2018 | Tony Brown Development. 🚀')
copy.appendChild(footerP);
footer.appendChild(copy);
footer.setAttribute('class', 'footer');

app.appendChild(logo);
app.appendChild(container);
document.body.appendChild(footer)

const url = 'https://ghibliapi.herokuapp.com/films'

fetch(url)
  .then((resp) => resp.json())
  .then(function(data) {
    data.forEach(movie => {
      const card = document.createElement('div');
      card.setAttribute('class', 'card')
      const h1 = document.createElement('h1')
      h1.textContent = movie.title
      const p = document.createElement('p')
      movie.description = movie.description.substring(0, 300)
      p.textContent = `${movie.description}...`

      container.appendChild(card);
      card.appendChild(h1)
      card.appendChild(p)
    })
  })
  .catch((err) => {
    const errorMessage = document.createElement('marquee')
    errorMessage.textContent = 'Sorry, failed to fetch data'
    app.appendChild(errorMessage)
  })

const request = new XMLHttpRequest();

request.open('GET', 'https://ghibliapi.herokuapp.com/films', true);

request.onload = function () {
  const data = JSON.parse(this.response);

  if (request.status >= 200 && request.status < 400) {
    data.forEach(movie => {
      const card = document.createElement('div');
      card.setAttribute('class', 'card');

      const h1 = document.createElement('h1');
      h1.textContent = movie.title;

      const p = document.createElement('p');
      movie.description = movie.description.substring(0, 300);
      p.textContent = `${movie.description}...`;

      container.appendChild(card);
      card.appendChild(h1);
      card.appendChild(p);
    })
  } else {
      const errorMessage = document.createElement('marquee');
      errorMessage.textContent = 'Sorry, failed to fetch data...';
      app.appendChild(errorMessage);
  }
}


request.send();