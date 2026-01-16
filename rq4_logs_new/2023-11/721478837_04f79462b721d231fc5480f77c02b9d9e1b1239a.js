import React from "react";
import Home from "./pages/Home";
import AddTodo from "./pages/AddTodo";
import "./App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";

function App() {
  return (
    <div className="App">
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/add" element={<AddTodo />} />
        </Routes>
      </BrowserRouter>
    </div>
  );
}

export default App;