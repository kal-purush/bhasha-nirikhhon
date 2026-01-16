import React from "react";
import "../styles/Floor.css";

const Floor = ({ floorNumber, onClick }) => {
  return (
    <div className="floor">
      <div className="floor__container">
        <div className="floor__buttons">
          <button className="floor__up-btn" onClick={onClick}>
            Up
          </button>
          <button className="floor__down-btn" onClick={onClick}>
            Down
          </button>
        </div>
        <h4 className="floor__number">{floorNumber}</h4>
      </div>
    </div>
  );
};

export default Floor;