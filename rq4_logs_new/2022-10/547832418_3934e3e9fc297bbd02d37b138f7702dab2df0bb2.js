import React from 'react';
import './firstTask.css';

function FirstTask(props) {
  return(
    <div className="main-wrapper">
      <h1>First task</h1>
      <div className="block-wrapper">
        <div className="container">
          <h2>Some info</h2>
          <div className="info-wrap">
            <div className="info-block">
              <ul>
                {
                 Object.keys(props.data).map(function (key) {
                   if (key !== 'photo') {
                     return (
                       <li key={key}>{key}: {props.data[key]}</li>
                     )
                   }
                 })
                }
              </ul>
            </div>
            <img src={process.env.PUBLIC_URL + props.data.photo} alt="image"/>
          </div>
        </div>
      </div>
    </div>
  )
}

export default FirstTask;