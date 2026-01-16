import { useState, useEffect } from "react";

function getWindowSize() {
  const { innerWidth: width } = window;
  return { width };
}

export default function useWindowSize() {
  const [windowSize, setWindowSize] = useState(getWindowSize());

  useEffect(() => {
    window.addEventListener('resize', setWindowSize(getWindowSize()))
  }, [])

  return windowSize;
}