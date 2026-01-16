import React from "react";
import "./style.css";

const Buttons = ({ tasks, hideDoneTasks }) =>
  tasks.length > 0 && (
    <div className="buttons">
      <button className="buttons__button">
        {hideDoneTasks ? "Show" : "Hide"} Done
      </button>
      <button
        className="buttons__button"
        disabled={tasks.every(({ done }) => done)}
      >
        Mark All Done
      </button>
    </div>
  );

export default Buttons;