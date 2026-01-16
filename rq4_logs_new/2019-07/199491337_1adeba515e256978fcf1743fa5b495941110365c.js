import React from 'react';
import './ColorGrid.css';

const ColorGrid = () => (
  <div className="color-container">
    <div className="color-row">
      <button id="swatch1" onclick="document.getElementById('mood').style.fill = 'red'"></button>
      <button id="swatch2" onclick="document.getElementById('mood').style.fill = 'blue'"></button>
      <button id="swatch3" onclick="document.getElementById('mood').style.fill = 'yellow'"></button>
      <button id="swatch4" onclick="document.getElementById('mood').style.fill = 'green'"></button>
      <button id="swatch5" onclick="document.getElementById('mood').style.fill = 'pink'"></button>
      <button id="swatch6" onclick="document.getElementById('mood').style.fill = 'orange'"></button>
    </div>
  </div>
)

export default ColorGrid;