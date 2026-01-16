import React from "react";

const Alert = (props) => {
  function Capitalize(word) {
    return word.charAt(0).toUpperCase() + word.slice(1);
  }
  return (
    props.alert && (
      <div>
        <div
          class={`alert alert-${props.alert.type} alert-dismissible fade show`}
          role="alert"
        >
          <strong>{Capitalize(`${props.alert.msg}`)}</strong>---
          {props.alert.type}
          <button
            type="button"
            class="btn-close"
            data-bs-dismiss="alert"
            aria-label="Close"
          ></button>
        </div>
      </div>
    )
  );
};

export default Alert;