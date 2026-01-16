import React from "react";
import Calendar from "./components/Calendar";
import "./styles.css";

export default function App() {
  return (
    <div className="App">
      <div className="calendar-wrapper">
        <Calendar />
      </div>
      <footer className="footer">
        Designed with{" "}
        <span style={{ color: "red", fontSize: "1.3em" }}>&#10084;</span> by
        <a href="https://twitter.com/dawoodshahat" target="_blank">
          Muhammad Dawood
        </a>
      </footer>
    </div>
  );
}