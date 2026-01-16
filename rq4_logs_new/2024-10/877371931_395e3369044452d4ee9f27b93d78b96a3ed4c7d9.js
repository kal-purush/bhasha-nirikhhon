import './App.css';
import React, { useState, useEffect, useMemo } from 'react';

const TIME_API_ENDPOINT = 'http://localhost:3000/time';
const METRICS_API_ENDPOINT = 'http://localhost:3000/metrics';
const AUTH_TOKEN = 'mysecrettoken';

function App() {

  const [clientTimeData, setClientTimeData] = useState(null);
  const [serverTimeData, setServerTimeData] = useState({data: {epoch: null} , isLoading: false});
  const [metricsData, setMetricsData] = useState({data: null , isLoading: false});

  const getData = async (api, state, setFunc) => {
    try {
      setFunc({...state, isLoading: true})
      const response = await fetch(api, {
        headers: {'Authorization': AUTH_TOKEN}
      });
      const contentType =response.headers.get("content-type");
      let content = null;
      if (contentType && contentType.indexOf("application/json") !== -1) {
        content = await response.json();
      } else {
        content = await response.text();
      };
      setFunc({data: content, isLoading: false})
    } catch (error) {
      console.error('Error loading %s data: %s', api, error);
      setFunc({...state, isLoading: false})
    }
  };

  useEffect(() => {

    // Initial data setup
    setClientTimeData(Date.now())
    getData(TIME_API_ENDPOINT, serverTimeData, setServerTimeData);
    getData(METRICS_API_ENDPOINT, metricsData, setMetricsData);

    // Set up interval for client time update
    const clientUpdateInterval = setInterval(() => {
      setClientTimeData(Date.now());
    }, 1000);

    // Set up interval for Server data update
    const serverUpdateInterval = setInterval(() => {
      getData(TIME_API_ENDPOINT, serverTimeData, setServerTimeData);
      getData(METRICS_API_ENDPOINT, metricsData, setMetricsData);
    }, 30000);

    return () => {
      clearInterval(clientUpdateInterval);
      clearInterval(serverUpdateInterval);
    }
  }, []);

  const formattedTimeDiff = useMemo(() => {
    const millisecDiff = Math.abs(clientTimeData - serverTimeData.data.epoch);
    return new Date(millisecDiff).toISOString().slice(11,19);
  }
  ,[clientTimeData, serverTimeData]);

  return (
    <div className="splitScreen">
      <div className="topPane">
        {!serverTimeData.isLoading &&
        <>
          <div>
            Server time: {serverTimeData.data.epoch}
          </div>
          <div>
            Time Difference: {formattedTimeDiff}
          </div>
        </>
        }
        {serverTimeData.isLoading &&
          <h2>
            Loading...
          </h2>
        }
      </div>
      <div className="bottomPane">
        {!metricsData.isLoading &&
          <pre><code>
            {metricsData.data}
          </code></pre>
        }
        {metricsData.isLoading &&
          <h2>
            Loading...
          </h2>
        }
      </div>
    </div>
  );
}

export default App;