import React from 'react';
import { BrowserRouter as Router, Routes, Route} from 'react-router-dom';
import './App.css';
import Home from './component/home';
import ApprofondimentiPage from './component/approfondimenti_page';

function App() {
  return (
    <Router basename={process.env.PUBLIC_URL}>
        {/* Routes */}
        <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/insight/:id" element={<ApprofondimentiPage />} />
        </Routes>
    </Router>
  );
}

export default App;