import React from "react";
import { NavLink } from "react-router-dom";



const Nav = (props) => {

    const handleClick = (e) => {
        let btnText = e.target.innerText;
        props.updateQuery(btnText);
    }

    return (
      <nav className="main-nav">
        <ul>
          <li onClick={handleClick}><NavLink to="/cats">Cats</NavLink></li>
          <li onClick={handleClick}><NavLink to="/dogs">Dogs</NavLink></li>
          <li onClick={handleClick}><NavLink to="/birds">Birds</NavLink></li>  
        </ul>
      </nav>  
        
    );
}

export default Nav;