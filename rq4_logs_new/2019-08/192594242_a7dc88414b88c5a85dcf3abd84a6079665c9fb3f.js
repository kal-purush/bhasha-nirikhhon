import React from 'react';

import './about.css';

const About = () => {
  return (
    <div id="about-container">
      <div id="about">
        <h4 id="about-header">Never Asked Questions</h4>
        <div className="naq">
          <div className="naq-text">
            <h5 className="naq-question">
              WHAT IS THIS WEBSITE ABOUT?
            </h5>
            <p className="naq-answer">
              This is a remake of the <a href="2019.e3recap.com">E3Recap</a> site.
              My goal was to practice and improve my frontend skills (javascript, html, and css).
              The site is narrow in scope, and is about something I'm interested in, and I'm not
              particularly skilled at web design so it seemed like the right kind of project.
              If you have somehow come to this website unintentionally, please visit the original site.
              If you are interested in the code, feel free to checkout the
              <a href="https://github.com/nav01/improved-disco"> git repository</a>.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}

export default About;