import React from "react"
import "./index.scss"

const SearchButton = props => {
  return (
    <div className="btn-wrap">
      <button className="searchButton" onClick={props.clicked}>
        Search
      </button>
      <button onClick={props.clickReset}>Reset</button>
    </div>
  )
}
export default SearchButton