import React from "react";
import { NavLink } from "react-router-dom";

function Error() {
  return (
    <div>
      <h3>404</h3>
      <p>UH OH! You're lost</p>
      <p>the page is looking for it donst exist </p>
      <p>Please click the home button</p>
      <NavLink to='/'>
        <button type='button' class='btn btn-primary'>
          GO BACK TO HOME
        </button>
      </NavLink>
    </div>
  );
}

export default Error;