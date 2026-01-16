import './App.css';
import React from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  Link
} from 'react-router-dom';
import Home from "./components/home.js";
import Online from "./components/online.js";


function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/online" element={<Online />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;