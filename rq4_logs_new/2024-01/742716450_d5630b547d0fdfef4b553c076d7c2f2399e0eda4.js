import React, { useState } from "react";
import classes from "./Header.module.css";
import logo from "../../assets/images/logo.svg";
import downArrow from "../../assets/images/down-arrow.svg";
import menuIcon from "../../assets/images/menu-icon.png"

export const Header = () => {
  const [show,setShow] = useState(false);

  const handleShow = () => {
    setShow(!show)
  }
  return (
    <header className={classes.header_main}>
      <img src={logo} alt="Logo" className={classes.logo} />
      <nav className={show ? classes.showNav : classes.hideNav}>
        <ul className={classes.nav_items_container}>
          <li>
            Solutions{" "}
            <img src={downArrow} alt="Arrow" className={classes.down_arrow} />
          </li>
          <li>
            Platform{" "}
            <img src={downArrow} alt="Arrow" className={classes.down_arrow} />
          </li>
          <li>
            Industries{" "}
            <img src={downArrow} alt="Arrow" className={classes.down_arrow} />
          </li>
          <li>
            Featured Insights{" "}
            <img src={downArrow} alt="Arrow" className={classes.down_arrow} />
          </li>
          <li>
            About Us{" "}
            <img src={downArrow} alt="Arrow" className={classes.down_arrow} />
          </li>
          <li>
            Careers{" "}
            <img src={downArrow} alt="Arrow" className={classes.down_arrow} />
          </li>
          <li>Our Work</li>
        </ul>
      </nav>
      <button className={classes.header_btn}>Free Consultation</button>
      <img src={menuIcon} alt="Menu Icon" className={classes.menu_icon} onClick={handleShow}/>
    </header>
  );
};