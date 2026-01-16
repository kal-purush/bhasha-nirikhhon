import React from 'react'
import './loading.css'

// https://loading.io/css/
const Loading = ({
  text
}) =>
  (
    <div className='loading'>
      <span>
        {text}
      </span>
      <div class="lds-roller">
        <div></div>
        <div></div>
        <div></div>
        <div></div>
        <div></div>
        <div></div>
        <div></div>
        <div></div>
      </div>
    </div>
  )
export default Loading