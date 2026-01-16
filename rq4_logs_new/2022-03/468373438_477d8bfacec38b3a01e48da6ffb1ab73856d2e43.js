import './App.css';
import { Route, Routes } from 'react-router';
import MenuBar from './Components/MenuBar';
import PageNotFound from './Components/PageNotFound';
import Home from './Components/Home';
import Login from './Components/Login';
import Header from './Components/Header';
import Register from './Components/Register';
import Dashboard from './Components/Dashboard';
import './index.css';  
import Success from './Components/success';
function App() {
  return (
    <>
    <MenuBar/>
    <Routes>
      <Route path ='/' element={<Home/>}/>
      <Route path="/login" element={<Login/>}/>
      <Route path ='/details' element={<Register/>}/>
      <Route path ='/dashboard' element={<Dashboard/>}/>
      <Route path='/success' element={<Success/>}/>
      <Route path ='*' element={<PageNotFound/>}/>
    </Routes>
    </>
  );
}

export default App;