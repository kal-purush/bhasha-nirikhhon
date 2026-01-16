import React from "react";
import ReactDOM from "react-dom";

import "./styles.css";

function App() {
  return (
    <div className="container">
      <img className="img-fluid" src="images/s-snake.png" alt="s-snake" />
      <img
        className="img-fluid logo"
        src="images/code-cobra-color.svg"
        alt="CodeCobra Logo"
      />
      <h1>Coming Soon</h1>
    </div>
  );
}

const rootElement = document.getElementById("root");
ReactDOM.render(<App />, rootElement);