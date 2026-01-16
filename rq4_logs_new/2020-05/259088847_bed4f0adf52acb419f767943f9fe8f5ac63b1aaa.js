"use strict";

import withStyle from "../../index";  ///

const RowsDiv = (properties) => {
  const { className, childElements } = properties;

  return (

    <div className={`${className} rows`}>
      {childElements}
    </div>

  );
};

export default withStyle(RowsDiv)`

  display: flex;
  flex-grow: 1;
  flex-direction: column;

`;