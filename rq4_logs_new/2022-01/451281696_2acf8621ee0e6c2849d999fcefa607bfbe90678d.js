import './App.css';
import Home from './pages/Home';
import Secondpage from './pages/Secondpage';
import Thirdpage from './pages/Thirdpage';
import Fourthpage from './pages/Fourthpage';
import Fifthpage from './pages/Fifthpage';
import Sixthpage from './pages/Sixthpage';
import Footer from './components/Footer'

import {BrowserRouter ,Routes,Route} from "react-router-dom";

function App() {

  return (
    <div className="App">
      <BrowserRouter>
       
        <Routes>
          <Route exact path="/" element={<Home />} /> 
          <Route exact path="/Secondpage" element={<Secondpage />} /> 
          <Route exact path="/Thirdpage" element={<Thirdpage />} />
          <Route exact path="/Fourthpage" element={<Fourthpage />} />
          <Route exact path="/Fifthpage" element={<Fifthpage />} />
          <Route exact path="/Sixthpage" element={<Sixthpage />} />

          

        </Routes>
        <Footer />
      </BrowserRouter>
      
      
    </div>
  );
}

export default App;