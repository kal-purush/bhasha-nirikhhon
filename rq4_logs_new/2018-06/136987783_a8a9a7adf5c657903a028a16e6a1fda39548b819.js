import React from 'react'
import './suggestions.css'



const Suggestions = (props) => {
  const options = props.results.map(artist => (
    <div key={artist.id} className="suggestion">
      <div className="suggestion--image">
        <img src={artist.images[0] ? artist.images[0].url : "http://via.placeholder.com/100X100" } alt="" />
      </div>
      <div className="suggestion--name">{artist.name}</div>
    </div>
  ))
  return <div>{options}</div>
  
}

export default Suggestions