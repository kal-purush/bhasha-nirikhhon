import React from "react";
import { BrowserRouter as Router, Route } from "react-router-dom";
import Home from "./components/pages/Home";
import Student from "./components/pages/Student";
import Instructor from "./components/pages/Instructor";
import Navbar from "./components/Navbar";
import Add from "./components/pages/Add"

function App() {
  return (
    <Router>
        <Navbar />
          <Route exact path="/" component={Home} />
          <Route exact path="/Student" component={Student} />
          <Route exact path="/Instructor" component={Instructor} />
          <Route exact path="/Add" component={Add} />
    </Router>
  );
}

export default App;
