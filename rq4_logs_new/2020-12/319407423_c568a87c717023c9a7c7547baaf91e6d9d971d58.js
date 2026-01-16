import React from "react";
import Container from "react-bootstrap/Container";
import Card from "react-bootstrap/Card";

export default function Connect() {
  return (
    <div className="ready-div">
      <div className="square-ready"></div>
    <div className="ready-container">  
        <p className="get-started-text">
          Ready for integration?
        </p>
        <button className="get-started-btn">Get Started</button>
    </div>
    </div>
  );
}