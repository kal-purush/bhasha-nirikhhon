import { Route, Routes } from "react-router-dom";
import Favourite from "./components/Favourite";
import Navbar from "./components/Navbar";
import Random from "./components/Random";
import WithoutChuck from "./components/WithoutChuck";


function App() {
  return (
    <div className="App">
      <Navbar />
      <Routes>
        <Route path='/' element={<Favourite />} />
        <Route path='/fav' element={<Favourite />} />
        <Route path='/random' element={<Random />} />
        <Route path='/without-chuck' element={<WithoutChuck />} />
        <Route path="*" element={<div>OOOPS! I can't find this page! Please use the commands from the navigation bar!</div>} />
      </Routes>
    </div>
  );
}

export default App;