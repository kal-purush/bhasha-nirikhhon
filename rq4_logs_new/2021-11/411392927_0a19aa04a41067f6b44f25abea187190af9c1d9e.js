import React from 'react';
import './About.css';
import profilePic from '../images/profile.jpeg';

const About = () => {
  return (
    <div className="about">
      <div className="about-content">
        <h1>
          Hello, my name is Jimmy Smith <br />
          I'm a <span>Web Developer</span>
        </h1>
        <img src={profilePic} alt="profile-pic" />
      </div>
    </div>
  );
};

export default About;