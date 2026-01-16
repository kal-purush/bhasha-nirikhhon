// Elements from index.html (1st page)
const explorePlaceholder = document.getElementById("explore-placeholder")
const displayMovieHtml = document.getElementById("display-movies")
const searchBtn = document.getElementById("search-btn")

// let watchlistMovies = JSON.parse(localStorage.getItem("watchlist"))
let watchlistMovies = JSON.parse(localStorage.getItem("watchlist"))
console.log(JSON.parse(localStorage.getItem("watchlist")))

// Hide placeholder, get general movie info from API & calling detailed info function
searchBtn.addEventListener("click", ()=> {
    explorePlaceholder.style.display = "none"
    
    let inputSearchValue = document.getElementById("search").value
    
    fetch(`https://www.omdbapi.com/?apikey=217a0b3d&s=${inputSearchValue}`)
        .then (res => res.json())
        .then (data => {
            getMovieFromSearch(data.Search) 
        })
})

// Function to get detailed movie info from API & rendering html to DOM
function getMovieFromSearch(movieArray){
    let moviesHtml = ""
    movieArray.forEach(function(movie){
        fetch(`https://www.omdbapi.com/?apikey=217a0b3d&i=${movie.imdbID}`)
            .then(res => res.json())
            .then(data => {
                moviesHtml += getMovieHtml(data)
                displayMovieHtml.innerHTML = moviesHtml
                })
    })
}

// Function that provides the html for movies to be rendered
function getMovieHtml(movie) {
    return `
                <div id="movie-container" class="display-horizontal">
                <img class="movie-poster" src="${movie.Poster}">
                <div id="movie-info">
                    <div class="display-horizontal">
                        <p>${movie.Title}</p>
                        <p>Rated: ${movie.Rated}</p>
                    </div>
                    <div class="display-horizontal">
                        <p>${movie.Runtime}</p>
                        <p>${movie.Genre}</p>
                        <button id="watchlist-btn" data-movie="${movie.imdbID}">
                            <i class="fa-solid fa-circle-plus"></i>
                            Watchlist
                        </button>
                     </div>
                    <p>${movie.Plot}</p>
                </div>
            </div>
    `
}     
  
// Get selected movies's ID to add to new array & save new array to localStorage
 document.addEventListener("click", function(e){
    
     let selectedMovie = e.target.dataset.movie
     
     if (selectedMovie) {
        watchlistMovies.push(selectedMovie)
        localStorage.setItem("watchlist", JSON.stringify(watchlistMovies))
     }
 }) 
 