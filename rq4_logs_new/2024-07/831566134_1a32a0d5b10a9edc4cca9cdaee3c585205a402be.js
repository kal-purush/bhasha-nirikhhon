import React, { useEffect, useState } from 'react';
import '../styles/HoppingImage.css';

const HoppingImage = () => {
  const [position, setPosition] = useState({ top: '0px', left: '0px' });
  const [step, setStep] = useState(0);

  useEffect(() => {
    const moveImage = () => {
      const container = document.querySelector('.background-container');
      const targetX = container.clientWidth / 2 - 50; // Center X position
      const targetY = container.clientHeight / 2 - 50; // Center Y position
      
      const x = (targetX / 10) * step;
      const y = (targetY / 10) * step;

      setPosition({ top: `${y}px`, left: `${x}px` });
      setStep(prevStep => (prevStep < 10 ? prevStep + 1 : 0)); // Increment step or reset
    };

    const interval = setInterval(moveImage, 500); // Change position every half second
    return () => clearInterval(interval);
  }, [step]);

  return (
    <div className="background-container">
      <img
        src="/lonelycut.png"
        alt="Hopping dog"
        className="hopping-image"
        style={{ ...position, animation: 'hop 0.5s infinite' }}
      />
    </div>
  );
};

export default HoppingImage;