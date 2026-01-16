import React from "react";
import "./styles.css";
const WithStyles = (WrappedComponent) => {
  return (props) => (
    // <div className={styles.wrappedStyles}>
      <WrappedComponent {...props} />
    // </div>
  );
};

export default WithStyles;