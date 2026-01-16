// App.js
import React, { useState, useEffect } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import Main from "./components/Main/Contents";
import Signup from "./components/SignUp/Signup";
import Login from "./components/Login/Login";
import "./App.css";

function App() {
  const [token, setToken] = useState(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const storedToken = localStorage.getItem("token");
    if (storedToken) {
      setToken(storedToken);
    }
    setIsLoading(false);
  }, []);
  
  if (isLoading) {
    return <div>Loading...</div>;
  }
  
  // console.log(token)
  return (
    <Routes>
      {token && <Route path="/" element={<Main />} />}
      <Route path="/signup" element={<Signup />} />
      <Route path="/login" element={<Login setToken={setToken} />} />
      <Route path="/*" element={<Navigate replace to="/login" />} />
    </Routes>
  );
}

export default App;