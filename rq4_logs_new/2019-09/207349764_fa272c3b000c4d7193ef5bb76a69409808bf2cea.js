import React from 'react';
import './quater-button.style.css';

function QuaterButton(props) {
  const nextQuater = () => {
    if (props.hook[0] >= 4) props.hook[1]('!!');
    else if (typeof props.hook[0] === 'number') props.hook[1](props.hook[0] + 1);
  };

  return (
    <button className='quaterButton' onClick={() => nextQuater()}>
      Next Quater
    </button>
  );
}

export default QuaterButton;