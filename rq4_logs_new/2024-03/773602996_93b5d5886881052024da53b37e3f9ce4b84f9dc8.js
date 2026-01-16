const header = 'Top Five Marvel Movies!';
document.querySelector('.header').innerHTML = header;

const movies = [
  {name: "Captain America The Winter Soldier", date: "3/13/2014", time: "136 minutes", boxOffice: "$714.4 million", imgUrl: "https://upload.wikimedia.org/wikipedia/en/9/9e/Captain_America_The_Winter_Soldier_poster.jpg"},
  {name: "Iron Man 2", date: "4/26/2010", time: "125 minutes", boxOffice: "$623.9 million", imgUrl: "https://upload.wikimedia.org/wikipedia/en/e/ed/Iron_Man_2_poster.jpg"},
  {name: "Spider-Man No Way Home", date: "12/17/2021", time: "148 minutes", boxOffice: "$1.922 billion", imgUrl: "https://upload.wikimedia.org/wikipedia/en/0/00/Spider-Man_No_Way_Home_poster.jpg"},
  {name: "Guardians of the Galaxy, Vol. 2", date: "5/5/2017", time: "136 minutes", boxOffice: "$869.8 million", imgUrl: "https://upload.wikimedia.org/wikipedia/en/3/32/Guardians_of_the_Galaxy_Vol._2_poster.jpg"},
  {name: "Thor Ragnarok", date: "11/3/2017", time: "130 minutes", boxOffice: "$865 million", imgUrl: "https://upload.wikimedia.org/wikipedia/en/7/7d/Thor_Ragnarok_poster.jpg"}
]

function createCardHTML(movie) {
  return `
    <div class="card">
      <img class="card-img" src="${movie.imgUrl}" alt="${movie.name}">
      <div class="card-details">
        <h2 class="card-title">${movie.name}</h2>
        <p class="card-date">Date: ${movie.date}</p>
        <p class="card-time">Duration: ${movie.time}</p>
        <p class="card-box">Box Office: ${movie.boxOffice}</p>
      </div>
    </div>
  `;
}

const cardsHTML = movies.map(createCardHTML).join("");
document.querySelector('.cards').innerHTML = cardsHTML;