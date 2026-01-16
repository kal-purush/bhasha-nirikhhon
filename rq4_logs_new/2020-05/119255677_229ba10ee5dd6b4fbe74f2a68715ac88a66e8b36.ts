import {useRef, useState, useEffect} from 'react';

export function useFrameRate(rollingMeanLen = 1, updateLen = 100) {
  const [frameRate, setFrameRate] = useState(60);
  const prevFrameTimestampRef = useRef<number | null>(null);
  const frameLengthsRef = useRef([...Array(rollingMeanLen)].map(() => 16.67));
  const lastUpdateRef = useRef(0);
  useEffect(() => {
    /*
     * Estimate and report current frame rate of app. Called with
     * requestAnimationFrame. Uses average frame length over
     * rollingMeanLen.
     */
    const updateFrameRate = (timestamp: number) => {
      if (prevFrameTimestampRef.current != null) {
        const frameLength = timestamp - prevFrameTimestampRef.current;
        frameLengthsRef.current.shift();
        frameLengthsRef.current.push(frameLength);
        const meanFrameRate = 1000 / frameLength;
        if (Date.now() - lastUpdateRef.current >= updateLen) {
          setFrameRate(meanFrameRate);
          lastUpdateRef.current = Date.now();
        }
      }
      prevFrameTimestampRef.current = timestamp;
      requestAnimationFrame(updateFrameRate);
    };
    requestAnimationFrame(updateFrameRate);
  }, [setFrameRate, updateLen]);

  return frameRate;
}