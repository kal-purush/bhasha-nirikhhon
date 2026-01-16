import React, { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom';
import { API_KEY, API_URL,IMAGE_BASE_URL } from '../../../Config';
import MainImage from '../LandingPage/Sections/MainImage';
import Movieinfo from './Sections/Movieinfo';
import GridCard from '../commons/GridCard';
import { Row } from 'antd';
import Favorite from './Sections/Favorite';

function MovieDetail(props) {
    let { movieId } = useParams();
    const [Movie, setMovie] = useState([]);
    const [Casts, setCasts] = useState([]);
    const [ActorToggle, setActorToggle] = useState(false);
    useEffect(() => {
        let endpointCrew = `${API_URL}movie/${movieId}/credits?api_key=${API_KEY}`;

        let endpointInfo = `${API_URL}movie/${movieId}?api_key=${API_KEY}`;
        
        fetch(endpointInfo)
            .then(response => response.json())
            .then(response => {
                setMovie(response)
            })
        
        
        fetch(endpointCrew)
        .then(response => response.json())
            .then(response => {
            setCasts(response.cast)
        })
    }, [])
    
    const toggleActorView = () => {
        setActorToggle(!ActorToggle);
    }
  return (
      <div>
          {/* Header */}
          {/* {console.log(Movie)} */}
          {Movie.backdrop_path &&
              <MainImage
                  image={`${IMAGE_BASE_URL}w1280/${Movie.backdrop_path}`}
                  title={Movie.original_title}
                  text={Movie.overview}
              />
          }
          {/* Body */}

          <div style={{ width: '85%', margin: '1rem auto' }}>
              <div style={{display:'flex',justifyContent:'flex-end'}}>
                  <Favorite movieInfo={Movie} movieId={movieId} userFrom/>
              </div>
              {/* Movie Info */}
              <Movieinfo
              movie={Movie}
              />
              <br />
              {/* Actor Grid */}
              <div style={{ display: 'flex', justifyContent: 'center', margin: '2rem' }}>
                  <button onClick={toggleActorView}>
                    Toggle Actor views
                  </button>

              </div>
              { ActorToggle &&
                  <Row gutter={[16,16]}>
                  {Casts && Casts.map((cast, index) => (
                      <React.Fragment key={index}>
                          {cast.profile_path&&
                          <GridCard
                              image={`${IMAGE_BASE_URL}w500${cast.profile_path}`}
                              castName={cast.name}
                              />
                            }
                      </React.Fragment>
                  ))}
              
              </Row>
             }
             

          </div>
      </div>
  )
}

export default MovieDetail