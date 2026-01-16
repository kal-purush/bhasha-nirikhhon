import React from "react";
import { useState } from "react";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import styles from "./Dropdown.module.css";

const Dropdown = (props) => {
  const [dropdownClicked, setDropdownClicked] = useState(false);
  const dropdownHandler = () => {
    dropdownClicked === false
      ? setDropdownClicked(true)
      : setDropdownClicked(false);
  };
  const dropdown = (
    <>
      {props.synonyms !== ""
        ? props.synonyms.map((val) => {
            return (
              <div key={Math.random()} className={styles["option"]}>
                {val}
              </div>
            );
          })
        : undefined}
    </>
  );

  return props.theWord === "" ? undefined : (
    <div className={styles["dropdown-container"]}>
      <div onClick={dropdownHandler} className={styles["select"]}>
        {props.theWord}
        <ArrowDownwardIcon
          sx={{
            width: "2vh",
            height: "3vh",
            marginLeft: "2px",
            borderLeft: "solid rgb(197, 193, 192) 1px",
          }}
        />
      </div>
      {dropdownClicked === true && dropdown}
    </div>
  );
};

export default Dropdown;