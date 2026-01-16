// React Import
import React from 'react';
import {Link} from 'react-router'

// CSS Import
import 'bootstrap/dist/css/bootstrap.css';
import './App.css';

// src={require('./logo-final-120.png')} 
var NavBar = React.createClass({
  render: function () {
    return (
      <div className="container-fluid nav-container" >
        <nav className="navbar navbar-fixed-top">
          <div className="navbar-header">
          <Link className="navbar-brand navImg" to="/">Commodifi Logo Small</Link>
            <button id="juno" type="button" className="navbar-toggle" data-toggle="collapse" data-target="#myNavbar">
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
            </button>
          </div>
          <div className="collapse navbar-collapse" id="myNavbar">
            <ul className="nav navbar-nav">
              <li><Link to="air">Air</Link></li>
              <li><Link to="honey">Honey</Link></li>
              <li><Link to="grains">Grains</Link></li>
              <li><Link to="seeds">Seeds</Link></li>
              <li><Link to="water">Water</Link></li>
            </ul>
          </div>
        </nav>
      </div>
    )
  }
})

export default NavBar


