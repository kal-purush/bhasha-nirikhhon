import "./App.css";
import Header from "./components/Header";
import Footer from "./components/Footer";
import Quest from "./components/Quests";
import Home from "./components/Home";
import Contact from "./components/Contact";
import CreateQuest from "./components/CreateQuest";
import { Routes, Route } from "react-router-dom";
import Login from "./components/Login";
import Signup from "./components/Signup";

function App() {
  return (
    <div>
      <div className="app-container">
        <Header />

        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/quests" element={<Quest />} />
          <Route path="/create-quest" element={<CreateQuest />} />
          <Route path="/contact" element={<Contact />} />
          <Route path="/login" element={<Login />} />
          <Route path="/signup" element={<Signup />} />
        </Routes>

        <Footer />
      </div>
    </div>
  );
}

export default App;