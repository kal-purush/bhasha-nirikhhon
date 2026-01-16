import logo from './logo.svg';
import './App.css';
import Navbar from './Components/Navbar';
import React, {Component} from "react";
import {BrowserRouter as Router, Route, NavLink, Switch} from 'react-router-dom'

import Signup from './Components/Signup/Signup'
import Signin from './Components/SignIn/Signin'
import Viewcar from './Components/Car/Viewcar';
import AddCar from './Components/Car/Addcar';
import EditCar from './Components/Car/Editcar';
import ProtectedRoute from './Components/ProtectedRoute';

function App() {

  // const login = localStorage.getItem('isLoggedIn')

  // let navLink = (
    
  //   <div className="Tab">
      
  //     <NavLink to="/login" activeClassName="activeLink" className="signIn">
  //       Sign In
  //     </NavLink>
  //     <NavLink exact to="/register" activeClassName="activeLink" className="signUp">
  //       Sign Up
  //     </NavLink>
  //   </div>
  // );
  return (
    <div className="App">
      <Router forceRefresh={true} >

      
        <Router>
        
        {/* {navLink} */}
        <Navbar />
          <Route  path='/register' component = {Signup} />
          <Route  path='/login' component={Signin} />
          <ProtectedRoute  path='/cars' component={Viewcar} />
          <ProtectedRoute  path="/add-car" component={AddCar} />
          <ProtectedRoute  path="/edit-car/:id" component={EditCar} />
       

        </Router>
        </Router>

        
      
    </div>
  );
}
export default App;

