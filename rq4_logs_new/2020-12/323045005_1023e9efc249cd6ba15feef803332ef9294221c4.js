import React from "react";
import logo from "../assets/150birds_logo.webp";
import { NavLink } from "react-router-dom";
import { Nav, Navbar } from "react-bootstrap";
import styled from "styled-components";

const StyledNav = styled(Nav)`
  display: flex;
  justify-content: space-around;
  flex-grow: 1;
`;
const StyledNavLink = styled(NavLink)`
  font-size: large;
  font-weight: bold;
  text-align: center;
  color: black;
  text-decoration: none;
  &:hover {
    text-decoration: none;
  }
`;
const Header = styled.div`
  height: auto;
  background: white;
`;
const navItems = [
  { title: "Service", path: "/service" },
  { title: "Dashboard", path: "/dashboard" },
  { title: "Conference", path: "/conference" },
  { title: "Profile", path: "/profile" },
  { title: "Settings", path: "/settings" },
];

const Navigation = () => {
  return (
    <Header>
      <Navbar collapseOnSelect expand="md" className="navbar">
        <Navbar.Brand className="ml-4">
          <img
            src={logo}
            width="70"
            alt="Logo"
            height="70"
            className="d-inline-block align-top"
          />
        </Navbar.Brand>
        <Navbar.Toggle aria-controls="responsive-navbar-nav" />
        <Navbar.Collapse
          id="responsive-navbar-nav"
          className="text-center"
          animation="false"
        >
          <StyledNav className="nav">
            {navItems.map((n, ind) => (
              <Nav.Item key={ind}>
                <StyledNavLink
                  to={n.path}
                  exact
                  activeStyle={{ color: "#ffc83f" }}
                >
                  {n.title}
                </StyledNavLink>
              </Nav.Item>
            ))}
          </StyledNav>
        </Navbar.Collapse>
      </Navbar>
    </Header>
  );
};

export default Navigation;