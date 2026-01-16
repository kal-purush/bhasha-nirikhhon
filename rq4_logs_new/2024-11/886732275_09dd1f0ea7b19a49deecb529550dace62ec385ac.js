import React from "react";
import { MdRestaurantMenu } from "react-icons/md";
import styles from "../styles/Header.module.css";

const Header = () => (
  <header className={styles.header}>
    <MdRestaurantMenu size={40} className={styles.icon} />
    <h1 className={styles.title}>Recipe Finder</h1>
  </header>
);

export default React.memo(Header);