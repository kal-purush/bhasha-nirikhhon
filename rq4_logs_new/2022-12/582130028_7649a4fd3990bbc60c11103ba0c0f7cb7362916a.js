import "./App.css";
import Signin from "./Pages/Auth/Signin/Signin";
import Signup from "./Pages/Auth/Sigup/Signup";
import Dashboard from "./Pages/Dashboard/Dashboard";
import { Route, Routes } from "react-router-dom";
import { useSelector } from "react-redux";

function App() {
  const user = useSelector((state) => state.userData.data.user);
  return (
    <div className="App">
      <Routes>
        <Route path="/" element={user ? <Dashboard /> : <Signin />} />
        <Route path="/signin" element={user ? <Dashboard /> : <Signin />} />
        <Route path="/signup" element={user ? <Dashboard /> : <Signup />} />
        <Route path="/dashboard" element={user ? <Dashboard /> : <Signin />} />
        <Route path="*" element={<h1>No such page Exist☹️</h1>} />
      </Routes>
    </div>
  );
}

export default App;