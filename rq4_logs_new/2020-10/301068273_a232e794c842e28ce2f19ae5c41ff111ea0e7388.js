import React, { useState } from 'react';
import Card from '../Card';
import './result.css'

const Result = (props) => {

    console.log("Rendering result:", props);

    const renderCards = () => {
        const cards = [];
        for (let i=0; i<props.votes.length; i++) {
            cards.push(<Card key={i} title={props.votes[i].name} value={props.votes[i].vote} />);
        }
        return cards;
    }

    return (
        <div className="poker-table">
            {renderCards()}
        </div>
    );
}

export default Result; 