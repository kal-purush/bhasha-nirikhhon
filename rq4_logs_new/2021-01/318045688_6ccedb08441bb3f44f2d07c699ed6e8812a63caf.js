import React from "react";
import Nav from "react-bootstrap/Nav";
// import "./style.css";

function Navbar() {
  return (
    <Nav className="justify-content-center nav-style">
      <Nav.Item>
        <Nav.Link href="/">About</Nav.Link>
      </Nav.Item>
      <Nav.Item>
        <Nav.Link href="/Portfolio">Portfolio</Nav.Link>
      </Nav.Item>
    </Nav>
  );
}

export default Navbar;