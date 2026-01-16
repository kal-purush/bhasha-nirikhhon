import React from 'react';

const AboutMe = (props) => {
  return (
    <div className="about-me container">
      <div className="header">
        <h2 className='title'>About Me</h2>
        <p>I dream of having a Lamborghini one day</p>
      </div>
      <div className="card">
        <div className="row">
          <div className="image-side-static col-md-6">
            {/* <video src='/images/logo/about-me-gaming.mp4' className="video-player" muted autoPlay loop/> */}
            <img className="card-img" src="/images/profile/about-me.jpg" />
          </div>

          <div className="description col-md-6 card-body">
            <h3>One like this!</h3>
            <p>
              That would be ideal for the type of car, fresh out of high school.
              However this is out of my range for now, so I'd like to get me a
              Toyota Celica. I am doing this my doing some detailing work on the
              side while continuing to complete my school work.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AboutMe;