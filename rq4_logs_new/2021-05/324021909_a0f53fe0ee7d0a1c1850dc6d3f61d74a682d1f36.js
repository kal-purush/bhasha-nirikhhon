import axios from 'axios';
import React, { useState, useEffect } from "react";
import { makeStyles } from "@material-ui/core/styles";
import { useParams } from "react-router-dom";

import "../MovieDetails/MovieDetails.scss";

const api_key  = "e68f0e35dcc5a1bd27bfaedc41d3c894";
const BASE_URL = "https://api.themoviedb.org/3";
const getImage = (path) => `https://image.tmdb.org/t/p/w300/${path}`;


const useStyles = makeStyles((theme) => ({ 
  header: {
    backgroundPosition: "right -200px top",
    backgroundSize: "cover",
    backgroundRepeat: "no-repeat"
  },

  custom_bg: {
    backgroundImage: "linear-gradient(to right, rgba(4.31%, 3.14%, 3.92%, 1.00) 150px, rgba(4.31%, 3.14%, 3.92%, 0.84) 100%)",
    display: "flex",
    justifyContent: "center",
    flexWrap: "wrap"
  }

}));

function MovieDetails() {
  const [detail, setDetail] = useState([]);
  const [image, setImage] = useState([]);
  const { movie_id } = useParams();

  const api = axios.create({ baseURL: BASE_URL });
  const getDetails = api.get(`movie/${movie_id}`, { params: { api_key } });
  const getImages = api.get(`/movie/${movie_id}/images`, { params: { api_key } })

  useEffect(() => {
    getDetails.then(res => {
      console.log(res.data)
      setDetail([res.data])
    })

    getImages.then(res => {
      console.log("images", res.data)
      setImage([res.data])
    })
  }, []);

  const classes = useStyles();

  return (
    <div>
      {detail.map((item, i) => (
        <div key={i}>
          {image.map((elem, i) => (
            <div key={i} style={{
              backgroundImage: `url(${getImage(elem.backdrops[0].file_path)})`,
              height: `${elem.backdrops[0].height}`
              }} 
              className={classes.header}
            >
            <div className={classes.custom_bg}>
              <div className="header-contents__container">
                <img className="header-contents__image" src={getImage(item.poster_path)} />
                <div className="header-contents__text">
                  <p>{item.title}</p>
                  <p>{item.overview}</p>
                </div>
              </div>
            </div>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

export default MovieDetails;