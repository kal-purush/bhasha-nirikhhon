import React from "react";

const GenresList = ({ handleGenreClick, genres }) => {
  
  return (
    <div className="row mt-3">
      <div className="col">
        <ul className="list-inline">
          {genres.map((genre) => (
            <li className="list-inline-item" key={genre.id}>
              <button
                className="btn btn-outline-info"
                onClick={() => handleGenreClick(genre.id)}
              >
                {genre.name}
              </button>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
};

export default GenresList;