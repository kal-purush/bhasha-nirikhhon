import "./styles.css";
import { useState, useEffect } from "react";

export default function App() {
  const [counter, setCounter] = useState(0);
  const handleClick = () => {
    setCounter((prevCount) => prevCount - 1);
  };
  useEffect(() => {
    setCounter(100);
  }, []);
  return (
    <div className="App">
      <h1>Hello CodeSandbox</h1>
      <h2>Start editing to see some magic happen!</h2>
      <button onClick={handleClick}>-</button>
      <h1>{counter}</h1>
      <button onClick={() => setCounter((prevCount) => prevCount + 1)}>
        +
      </button>
    </div>
  );
}