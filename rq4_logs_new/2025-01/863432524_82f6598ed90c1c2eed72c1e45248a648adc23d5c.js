import React, { useEffect, useState } from "react";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

// Register Chart.js components
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

const EEGGraph = ({ wsUrl }) => {
  const [data, setData] = useState([]);
  const [timestamps, setTimestamps] = useState([]);
  const [chNames, setChNames] = useState([]);
  const [winsize, setWinsize] = useState(0);

  useEffect(() => {
    const socket = new WebSocket(wsUrl);
    console.log("[WebSocket] Connecting to WebSocket...");

    socket.onmessage = (event) => {
      const message = JSON.parse(event.data);
      console.log("[WebSocket] Received data:", message);

      // **Highlight**: Debugging data lengths
      console.log("Timestamps Length:", message.timestamp?.length);
      console.log("Channels Length:", message.data?.length);

      setData(message.data);
      setTimestamps(message.timestamp);
      setChNames(message.ch_names);
      setWinsize(message.winsize);
    };

    socket.onopen = () => console.log("[WebSocket] Connection opened.");
    socket.onclose = () => console.log("[WebSocket] Connection closed.");

    return () => {
      console.log("[WebSocket] Closing WebSocket...");
      socket.close();
    };
  }, [wsUrl]);

  // **Highlight**: Conditional render if data is not ready
  if (!data.length || !timestamps.length || !chNames.length) {
    return <div>Loading EEG Data...</div>;
  }

  const generateChartData = (channelData, channelName) => {
    return {
      labels: timestamps,
      datasets: [
        {
          label: channelName,
          data: channelData,
          borderColor: "blue",
          borderWidth: 1.5,
          tension: 0.1,
          pointRadius: 0,
        },
      ],
    };
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false, // **Highlight**: Ensure the graph is responsive
    scales: {
      x: { title: { display: true, text: "Timestamp (LSL Time)" } },
      y: { title: { display: true, text: "Amplitude" } },
    },
  };

  return (
    <div>
      <h2>Real-Time EEG Data</h2>
      {chNames.map((chName, idx) => (
        <div key={idx} className="chart-container" style={{ marginBottom: "20px" }}>
          <h3>{chName}</h3>
          <Line data={generateChartData(data[idx], chName)} options={chartOptions} />
        </div>
      ))}
    </div>
  );
};

export default EEGGraph;