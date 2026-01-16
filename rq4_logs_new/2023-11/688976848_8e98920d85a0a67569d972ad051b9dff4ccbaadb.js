import React from "react";
import './studentInfo.css';

export function StudentInfo() {
  return (
    <main style={{ color: '#090503ff' }}>
      <div className="aboutHeading">
        <h1 className="title">COM S 319: Construction of User Interfaces</h1>
        <p>Professor Abraham Netzahualcoy Aldaco Gastelum</p>
        <p>September 23, 2023</p>
      </div>

      <div className="aboutBody">
        <h2 className="dev">Developers:</h2>

        <div className="student">
          <h3>Alexis Feeney</h3>
          <p><a href="mailto:afeeney9@iastate.edu">afeeney9@iastate.edu</a></p>
        </div>

        <div className="student">
          <h3>Ryan Lowe</h3>
          <p><a href="mailto:rrlowe02@iastate.edu">rrlowe02@iastate.edu</a></p>
        </div>
      </div>

      <div style={{ width: '100%', height: '20vw', backgroundColor: 'rgba(255, 192, 203, 0.3)', position: 'relative' }}>
        <img id="bear" src="./images/characters/chefBearNoBG.png" alt="bear dressed as chef" style={{ height: '20vw', width: 'auto', position: 'absolute' }} />
      </div>
    </main>
  );
}