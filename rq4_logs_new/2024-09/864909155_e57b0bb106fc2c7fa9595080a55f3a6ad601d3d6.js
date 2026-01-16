import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import Home from './Components/Home/Home'; // Corrected path
import PTPR from './Components/PTPR/PTPR';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Home />} />
        {/* Route for PTPR page */}
        <Route path="/ptpr" element={<PTPR />} />
      </Routes>
    </Router>
  );
}

export default App;