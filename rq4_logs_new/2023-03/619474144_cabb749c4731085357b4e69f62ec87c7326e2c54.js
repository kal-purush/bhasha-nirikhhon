import { useState, useEffect } from "react";
import EyeIcon from "./svgs/eyeIcon";

export default function CountVisit({ apiUrl }) {
  const [count, setCount] = useState(0);

  useEffect(() => {
    async function fetchCount() {
      const response = await fetch(apiUrl);
      const data = await response.json();
      setCount(data.value);
    }
    fetchCount();
  }, [apiUrl]);

  return (
    <div className="flex items-center p-2">
      <EyeIcon />
      <span className="text-lg px-2 font-bold font-neuton opacity-70">
        {count}
      </span>
    </div>
  );
}