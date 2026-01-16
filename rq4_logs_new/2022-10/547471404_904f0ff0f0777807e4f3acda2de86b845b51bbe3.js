import React from "react";
import { Link } from "react-router-dom";

import classes from "./Error.module.css";

const Error = (props) => {
  return (
    <main>
      <p className={classes.heading}>{props.heading}</p>
      <div className={classes["sub-heading-container"]}>
        <p className={classes["sub-heading"]}>{props.subHeading1}</p>
        <p className={classes["sub-heading"]}>
          {props.subHeading2[0]}
          <Link to="/">{props.subHeading2[1]}</Link>
          {props.subHeading2[2]}
        </p>
      </div>
    </main>
  );
};

export default Error;