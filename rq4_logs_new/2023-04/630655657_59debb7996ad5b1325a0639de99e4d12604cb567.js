import React, { useState, useEffect } from "react";
// import { makeStyles } from "@material-ui/core/styles";
import Grid from "./Grid";
import "./styles.css";

// const useStyles = makeStyles((theme) => ({}));

/*

State:
Current Color = string
Grid = [string of hex values]

Components:
Grid 
Cells

Functions:
click handler on cell to set color 
*/

export default function App() {
  const [selectedColor, setSelectedColor] = useState("#fff");
  const [pixelSize, setPixelSize] = useState(50);
  const [boardSize, setBoardSize] = useState(10);

  function handleColorChange(e) {
    setSelectedColor(e.target.value);
  }

  function handlePixelChange(e) {
    setPixelSize(parseInt(e.target.value, 10));
  }

  function handleBoardSizeChange(e) {
    if (parseInt(e.target.value, 10) < 5) return;
    setBoardSize(parseInt(e.target.value, 10));
  }

  useEffect(() => {
    console.log("boardSize", boardSize);
  }, [boardSize]);
  // const classes = useStyles();

  return (
    <div>
      <section className="header">
        <input
          type="color"
          value={selectedColor}
          onChange={handleColorChange}
        />
        <input type="number" value={pixelSize} onChange={handlePixelChange} />

        <input
          type="number"
          value={boardSize}
          onChange={handleBoardSizeChange}
        />
      </section>

      <Grid
        selectedColor={selectedColor}
        boardSize={boardSize}
        pixelSize={pixelSize}
      />
    </div>
  );
}