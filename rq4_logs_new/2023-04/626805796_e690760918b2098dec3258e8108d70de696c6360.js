import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Login from "./components/Login";
import CreateAccount from "./components/CreateAccount";
import Home from "./components/Home";
import Broadcasts from "./components/Broadcasts";
import Navbar from "./components/Navbar";
import Admin from "./components/Admin";
import "./style.css";

function App() {
  return (
    <Router>
      <div className="App">
        <Navbar />
        <Routes>
          <Route path="/" element={<Login />} />
          <Route path="/createaccount" element={<CreateAccount />} />
          <Route path="/home" element={<Home />} />
          <Route path="/broadcasts" element={<Broadcasts />} />
          <Route path="/admin" element={<Admin />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;