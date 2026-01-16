import React from "react";
import Header from "./components/Header";
import Result from "./components/Result";
import CompareAccuracy from "./components/ComapreAccuracy";
import Details from "./components/Details";
import TimeTaken from "./components/timeTaken";


export default function App() {
  return (
    <div style={appStyle}>
      <Header />
      <div style={layoutStyle}>
        <Result />
        <div style={mainContent}>
          <CompareAccuracy />
          <Details />
        <TimeTaken/>
          </div>
      </div>
    </div>
  );
}

const appStyle = {
  fontFamily: "Arial, sans-serif",
  background: "#f9f9f9",
  padding: "20px",
};

const layoutStyle = {
  display: "flex",
  gap: "20px",
  marginTop: "20px",
};

const mainContent = {
  flex: "1",
};
