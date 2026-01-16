import React, { Component } from 'react'
import { Col, Navbar, Nav } from "react-bootstrap";
import { Link } from "react-router-dom";
import { HashLink } from 'react-router-hash-link';
import { ReactComponent as Logo } from "../img/logo-blanco.svg";
import { HamburgerSlider } from 'react-animated-burgers'

export class Menu extends Component {
    
state = {
    isActive: false
  }

  toggleButton = () => {
    this.setState({
      isActive: !this.state.isActive
    })
  }
  
  render(){
      const logoColor = this.props.logoWhite ? "blanco" : "";
      const textColor = this.props.textWhite ? "amarillo" : "";
      return (   
        <>
        {this.state.isActive && this.props.background && <div className="menu-overlay"></div>}
        <Navbar expand="md" className="px-0 py-4">

            <Col xs={6} md={2} className="p-0">
                <Link to="/">
                  <Logo className={`logo ${logoColor}`} />
                </Link>
            </Col>

            <Col xs={6} md={6} className="p-0 text-right">
                <Navbar.Toggle aria-controls="basic-navbar-nav" className="navbar-toggle collapsed p-0" onClick={this.toggleButton} >
                    <HamburgerSlider isActive={this.state.isActive} barColor="white" />
                </Navbar.Toggle>
            </Col>
            <div className={`menu-desplegable ${textColor}`}>
                <Col xs={12} md={1}>
                    <Navbar.Collapse id="basic-navbar-nav">
                        <Nav className="ml-auto text-right">
                            <Link to="/work">Work</Link>
                        </Nav>
                    </Navbar.Collapse>
                </Col>
                <Col xs={12} md={1}>
                    <Navbar.Collapse id="basic-navbar-nav">
                        <Nav className="ml-auto text-right">
                            <Link to="/about">About</Link>
                        </Nav>
                 </Navbar.Collapse>
                </Col>
                <Col xs={12} md={1}>
                    <Navbar.Collapse id="basic-navbar-nav">
                        <Nav className="ml-auto text-right">
                            <HashLink to="/about#clients">Clients</HashLink>
                        </Nav>       
                    </Navbar.Collapse>
                </Col>
                <Col xs={12} md={1}>
                    <Navbar.Collapse id="basic-navbar-nav">
                        <Nav className="ml-auto text-right">
                            <HashLink to="/about#contact">Contact</HashLink>
                        </Nav>
                    </Navbar.Collapse>
                </Col>
            </div>
        </Navbar>
        </>
      )
  }
}