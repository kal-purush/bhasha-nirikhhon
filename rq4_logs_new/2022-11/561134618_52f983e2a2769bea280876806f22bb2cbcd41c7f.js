const fetchData = async function () {

  const response = await fetch("https://ghibliapi.herokuapp.com/films");
  const data = await response.json();

  console.log(data)

  const randomInteger = function (max) {
    const randomInteger = Math.floor(Math.random() * max);
    return randomInteger;
  }
  
  
  let movie = data[randomInteger(data.length)];
  let body = document.querySelector('body');


  output =
    `
    <article>

      <h1>${movie.title}</h1>
      <h3 class="h3">${movie.original_title_romanised}</h3>
      <img src="${movie.image}" alt"Picture of ${movie.title}">
      <ul>
        <li><b>Original Title</b>: ${movie.original_title}
        <li><b>Director</b>: ${movie.director}</li>
        <li><b>Producer(s)</b>: ${movie.producer}</li>
        <li><b>Release Date (Year)</b>: ${movie.release_date}</li>
        <li><b>Runtime</b>: ${movie.running_time} minutes</li>
        <li><b>Review Score</b>: ${movie.rt_score}% on Rotten Tomatoes</li>
        <li><b>Plot Summary</b>:<em> ${movie.description}</li> 
      </ul>

    </article>
  `

  body.innerHTML = output;
}


fetchData('https://ghibliapi.herokuapp.com/films');





//Console Log
console.log("Thank you for reading and enjoy the films!")
console.log("For more information click", 'https://ghibliapi.herokuapp.com/films');