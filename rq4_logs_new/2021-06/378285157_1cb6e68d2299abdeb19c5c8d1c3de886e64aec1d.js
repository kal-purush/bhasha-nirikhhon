import React from 'react';
import MovieCard from "./MovieCard";

function Filter({movies}) {

    let moviesArray = movies.map((movie) => (
        <MovieCard movie={movie}/>));

    function compare(a, b) {
        if(((a.props.movie.name).toLowerCase()) < (b.props.movie.name).toLowerCase()) { return -1; }
        if(((a.props.movie.name).toLowerCase()) > (b.props.movie.name).toLowerCase()) { return 1; }
        return 0;
    }

    //  console.log(moviesArray.map((movie)=>(movie.props.movie.rate)));
    // console.log(moviesArray);
     console.log(moviesArray.sort(compare));


    return (
        <div>
            <ul>
                {(moviesArray.sort(compare))}
            </ul>

        </div>
    );
};

export default Filter;