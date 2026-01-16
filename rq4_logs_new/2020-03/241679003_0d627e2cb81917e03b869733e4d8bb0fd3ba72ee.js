import React from "react";

const SearchButton = props => {
    return (
      <div >
        <button className={props.type} onClick={props.function}>{props.name}</button>
        
      </div>
    );
  };
  
  export default SearchButton;