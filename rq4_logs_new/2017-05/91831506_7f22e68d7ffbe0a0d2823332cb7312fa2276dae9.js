import React from 'react';
import './App.css';
import hawaii from './images/map.jpg';


function Transport(props) {
  return (

    <div>
      <div className="transport-inform" style={{
        backgroundImage: `url(${hawaii})`,
        backgroundSize: 'cover',
      }}>
      </div>
      <h2>Good Morning {props.name}, you have been transported to Hawaii. Congratulations! </h2>
      <img src={hawaii} className="welcomeImage" />
      <h2>Send A message to all your rained out PDX homies: {props.message}</h2>
      <form className="message-form"
        onSubmit={event => {
          event.preventDefault();
          props.changeMessage(event.target.elements.message.value);
        }}
      >
        <input style={{
          fontSize: '15px',
          borderRadius: '5px',
          backgoundColor: 'while'
        }}
          name='message'
          type='text'
          placeholder='Message To Homies:' />
        <input style={{
          fontSize: '15px',
          borderRadius: '5px',
          backgroundColor: 'white'
        }}
          type='submit'
          name='submit'
        />
      </form>
    </div >

  )
}

export default Transport;