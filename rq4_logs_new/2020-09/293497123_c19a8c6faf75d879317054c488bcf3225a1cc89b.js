import React from 'react'

const Details = (props) => {
    return(
        <div>
            <p>Name: {props.name}</p>
            <input id="name"></input>
            <p>Age: {props.age}</p>
            <input id="age"></input>
            
        </div>
    )
}

export default Details