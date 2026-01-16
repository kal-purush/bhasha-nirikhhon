import React from 'react'
import { BrowserRouter as Router, Routes , Route } from "react-router-dom"
import Navbar from "./components/Navbar";
import Home from './page/Home'
import Notes from './page/Notes'
import CreateNotes from './page/CreateNotes';


function App() {
  return (

    <Router>
      <Navbar />
      <Routes>
        <Route path="/" element={<Home />}/>
        <Route path="/notes" element={<Notes />}/>
        <Route path="/createnotes" element={<CreateNotes />}/>
      </Routes>
    </Router>


  );
}

export default App;