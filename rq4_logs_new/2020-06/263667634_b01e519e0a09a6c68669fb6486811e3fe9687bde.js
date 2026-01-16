import React from "react"
import ReactLoading from "react-loading"

import "./styles.css"

export default function () {
  return (
    <div className="loading-card">
      <ReactLoading type="spinningBubbles" color="#19857b" />
    </div>
  )
}