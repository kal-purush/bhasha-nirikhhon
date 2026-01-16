import { Col, Card, Row, Container } from "react-bootstrap";
import Paginate from "./pagination";
import { Link } from "react-router-dom";
import { useDispatch,useSelector } from "react-redux";
import { useEffect, useState } from "react";
import {getallmovies} from "../redux/actions/moviesaction";
import { motion } from "framer-motion"
import Titles from "./Titles";
export default function FullView(){
    const [data, setData] = useState([]);

    const dispatch = useDispatch();

    //render the api for one time 
    useEffect(() => {
        dispatch(getallmovies());
    }, [dispatch]);

    // Select movies from state
    const { movies } = useSelector((state) => state.movies); 

    // Set data when movies update
    useEffect(() => {
        if (movies && movies.length > 0) {
            setData(movies);
        }
    }, [movies]);

    return(
        <>
            <Container>
            <Titles />
                <Row>
                    {data.length>=1?(data.map((movie)=>{
                        return(
                                <Col sm="12" md="6" lg="3" key={movie.id} >
                                    <Link to={`/movie-details/${movie.id}`}>
                                        <motion.div
                                            initial={{ opacity: 0, translateY: 0 }}
                                            transition={{ duration: 0.5 }}
                                            whileInView={{ opacity: 1,translateY: 30 }}
                                            whileHover={{ scale: 1.05 }}
                                        >
                                            <Card className="full-card">
                                                <Card.Img className="h-100" variant="top" src={`https://image.tmdb.org/t/p/original`+movie.poster_path} />
                                                <Card.Body className="card-body text-center d-flex flex-column justify-content-center">
                                                    <div className="over-text">
                                                        <Card.Title className="movieTitle">{movie.original_title}</Card.Title>
                                                        <p>Release_date: {movie.release_date}</p>
                                                        <p>Number of voters: {movie.vote_count}</p>
                                                        <p>Rating: {movie.vote_average}</p>
                                                    </div>
                                                </Card.Body>
                                            </Card>
                                        </motion.div>
                                    </Link>
                                </Col>
                        )
                    })) : (
                        <h3 className="text-center">no movies now...</h3>
                    )}
                    {data.length>=1?(<Paginate/>):(null)}
                    
                </Row>
            </Container>
        </>
    )
}