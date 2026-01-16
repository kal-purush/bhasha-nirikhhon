
// import { useState } from 'react';
// import './Card.css';
export default function card(props) {


    const renderBirdCard = props.birds.map((bird, index) => {
        return (
            <div className="birdCardContainer">
                <div className='birds'>
                    <h6 className="birdName">{bird.name}</h6>
                    <p className="amount">Price: ${bird.amount}</p>
                    <img className="birdImg" src={bird.img} alt="somebird"></img>
                    <br></br>
                    <button className='birds'
                        onClick={() => props.add(bird)

                        }
                    >Adopt
                    </button>
                </div>
            </div>
            // console.log(bird.name)


        )

    })

    return (
        <div>
            {renderBirdCard}
        </div>
    )





}