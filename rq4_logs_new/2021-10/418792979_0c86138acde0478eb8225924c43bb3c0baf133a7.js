import "./styles.css";
import { useState } from "react";

export default function App() {
  const [count, setCount] = useState(0);
  const [data, setData] = useState([]);

  const onClickCount = () => {
    setCount(count + 1);
    setData([...data, count + 1]);
  };

  return (
    <div className="App">
      <h1 onClick={onClickCount}>{count}</h1>
      {[...Array(count)].map((_, i) => (
        <div>{i}</div>
      ))}
    </div>
  );
}