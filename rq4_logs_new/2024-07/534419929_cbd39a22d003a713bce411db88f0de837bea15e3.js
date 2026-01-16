import {
  Routes,
  Route,
  BrowserRouter,
} from "react-router-dom";
import "./App.css";
import Home from "./home/Home";
import Search from "./search/Search";
import StreetHome from "./streetHome/StreetHome";
import StreetHistory from "./streetHistory/StreetHistory";
import Faq from "./FAQ/FAQ";
function App() {
  return (
    <div className="App">
      <BrowserRouter>
        <Routes>
          <Route path="*" element={<Home />} />
          <Route path="/" element={<Home />} />
          <Route path="/search" element={<Search />} />
          <Route path="/street/:streetId/:number" element={<StreetHome />} />
          <Route path="/history/:streetId/:number" element={<StreetHistory />} />]
          <Route path="/faq" element={<Faq />} />

        </Routes>
      </BrowserRouter>
    </div>
  );
}

export default App;