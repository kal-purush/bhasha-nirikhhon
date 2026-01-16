import React from "react";
import { useState } from "react";

const LogicCheckBox = ({ label, value, onChange }) => {
  const [cbxInput, setCbxInput] = useState(value);

  return (
    <div>
      <label htmlFor="cbx">
        {label}
        <input
          id="cbx"
          type="checkbox"
          value={cbxInput}
          onChange={(e) => {
            setCbxInput(e.target.value);
            if (typeof onChange === "function") {
              onChange(e.target.value);
            }
          }}
        />
      </label>
    </div>
  );
};

export default LogicCheckBox;