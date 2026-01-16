import './App.css';
import logo from './logo.svg';
import { Routes, Route, BrowserRouter } from "react-router-dom";
import './App.css';
import '@radix-ui/themes/styles.css';
import Pages from './Pages/Pages';


function App() {
  return (
 
    <BrowserRouter>
    <Routes>
      <Route path="/" element={<Pages/>} caseSensitive={true} />
    </Routes>
  </BrowserRouter>
  );
}

export default App;