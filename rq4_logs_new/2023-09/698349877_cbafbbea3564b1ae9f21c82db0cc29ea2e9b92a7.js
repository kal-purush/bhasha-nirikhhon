import React from "react";
//import "src/App.js";
import { useState, useEffect } from "react";

var colors = [
  "#16a085",
  "#27ae60",
  "#2c3e50",
  "#f39c12",
  "#e74c3c",
  "#9b59b6",
  "#FB6964",
  "#342224",
  "#472E32",
  "#BDBB99",
  "#77B1A9",
  "#73A857"
];

const App = () => {
  const [Quote, setQuote] = useState([]);
  const [bgColor, setbgColor] = useState("'#16a085'");

  function randomColor() {
    let Color = colors[Math.floor(Math.random() * colors.length)];
    setbgColor(Color);
    data();
  }

  function data() {
    fetch("https://api.quotable.io/random")
      .then((res) => res.json())
      .then((Actdata) => setQuote(Actdata));
  }

  useEffect(() => {
    data();
  }, []);

  return (
    <div id="main" style={{ background: bgColor }}>
      <div id="wrapper">
        <div className="quote-text">{Quote.content}</div>
        <div className="quote-author">{Quote.author}</div>
      </div>
      <button className="button" id="new-quote" onClick={randomColor}>
        Click Me for new quote
      </button>
    </div>
  );
};

export default App;