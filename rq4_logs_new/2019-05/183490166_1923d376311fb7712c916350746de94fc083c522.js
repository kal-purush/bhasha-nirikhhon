import React, { useState } from 'react';

// import * as mm from '@magenta/music';

function MLButton() {
  const [pressed, setPressed] = useState(false);

  function predict() {
    setPressed(true);
    console.log('predicting...')
    setTimeout(function() {
      setPressed(false)
    }, 2000);
  }

  return (
    <div>
      <button
        onMouseDown={() => predict()}
        onTouchStart={() => predict()}
        className={pressed ? "synth-button mlbutton pressed" : "synth-button mlbutton"}
      >
      <i className="fas fa-brain"></i>
      </button>
    </div>

  )
}

export default MLButton;