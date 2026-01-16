import React from "react";
import logo from "./logo.svg";
import "./App.css";
import TextBox from "./TextBox";
import Button from "./Button";

function onClickValueParent() {
  alert("Clicked from Parent");
}

const ButtonValue = "Button Click Constant";

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.js</code> and save to reload.
        </p>

        <TextBox myValue="Surendar" anotherType="text" />

        <Button myvalue='with click event' onClickEvent={onClickValueParent} />

        <Button myvalue={ButtonValue} />
      </header>
    </div>
  );
}

export default App;