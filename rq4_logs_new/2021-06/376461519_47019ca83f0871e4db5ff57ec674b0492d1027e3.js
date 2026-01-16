import React from "react";
import Aux from "../../../HOC/AUX/aux";
import Classes from "./modal.module.css";

const modal = ({ show, children }) => {
  console.log("hear it is");
  return (
    <Aux>
      <div
        className={Classes.Modal}
        style={{
          transform: show ? "translateY(0)" : "translateY(-100hvh)",
          opacity: show ? "1" : "0",
        }}
      >
        {children}
      </div>
    </Aux>
  );
};

export default modal;