import logo from './logo.svg';
import './App.css';
import "../node_modules/bootstrap/dist/css/bootstrap.min.css";
import Navbar from './layout/Navbar';
import Home from './Pages/Home';
import { BrowserRouter as Router ,Routes,Route } from 'react-router-dom';
import AddToDo from './ToDoLists/AddToDo';
import EditToDo from './ToDoLists/EditToDo';
import Footer from './layout/Footer';

function App() {
  return (
    <div className="App">
    <Router>


      <Navbar></Navbar>

      <Routes>
        <Route exact path="/" element={<Home/>}/>
        <Route exact path="/addtodo" element={<AddToDo/>}/>
        <Route exact path="/tohome" element={<Home/>}/>
        <Route exact path="/edit/:id" element={<EditToDo/>} />


      </Routes>
    

    <Footer></Footer>
      </Router>
      
    </div>
  );
}

export default App;