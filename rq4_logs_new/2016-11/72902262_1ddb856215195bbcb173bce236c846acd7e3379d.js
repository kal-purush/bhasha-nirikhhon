import React, { Component } from 'react';
import { Entity } from 'aframe-react';

const EndGame = (props) => {
  const { restartDeck, score } = props;
  return (
    <Entity geometry={{ primitive: 'box', width: 10, height: 4,depth:0.3}} position={[-0.2, 2.09, 12.48]} material={{ color: '#99A7AA' }}>
      <Entity text={{ text: "Restart?" }} onClick={restartDeck} material={{ color: 'orange' }} position={[1.3, -0.7, -0.45]} rotation={[0, 180, 0]}  >
      </Entity>
      <Entity text={{text: `Final Score: ${score}`}} material={{ color: 'orange' }} position={[2.33, 0.73, -0.22]} rotation={[0, 180, 0]} >
      </Entity>
    </Entity>
  )
}

export default EndGame;