import React, { useState } from 'react';

function Flashcard(props) {
  const [isFront, setIsFront] = useState(true);

  const handleFlip = () => setIsFront(!isFront);

  return (
    <div className="flashcard">
      <div className={`flashcard-inner ${isFront ? 'front' : 'back'}`}>
        <p>{isFront ? props.question : props.answer}</p>
      </div>
    </div>
  );
}

export default Flashcard;