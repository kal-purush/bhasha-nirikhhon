import React from 'react';
import './Card.css'

const Card = props => {
    return (
        <div className="card">
            <h2 className="title">{props.title}</h2>
            <div className="picture"></div>
            <p className="rating">Rating: <span>{props.rating}</span></p>
            <p className="year">Year: <span>{props.year}</span></p>
        </div>
    );
}

export default Card;