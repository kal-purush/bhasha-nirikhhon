import React, { Component } from 'react';
import './hobbies.css';

class Hobbies extends Component {
  render() {
    return (
        <div>
          <h1>Hobbies</h1>
            <div class ="hobbie">
              <h3> Team Sports </h3>
              I love sports because they are not only good exercise, but also a group bonding experience.
              <br></br>
              <br></br>
              These days you can usually find me playing Ultimate Frisbee or Basketball.
              <br></br>
              <br></br>
              <img src='./images/UltiChampions.jpg' style={{width: 600}} alt='Team Photo' />
            </div>


            <div class ="hobbie">
              <h3> Enjoying Nature </h3>
              There's nothing more relaxing for me than heading down to the beach or going for a walk.
              <br></br>
              <br></br>
              <img src='./images/Beach.jpg' width={600} height = {450} alt='Beach' />
            </div>
        </div>
    );
  }
}

export default Hobbies;