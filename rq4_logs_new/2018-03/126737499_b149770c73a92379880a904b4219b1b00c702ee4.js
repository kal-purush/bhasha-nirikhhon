import React, {Component} from 'react';

// The nav bar
const Navbar = () => {
    return(
        <nav className="navbar">
            <div className="navbar__logo">
                <img alt="Brand" src="./logo_brand.svg" />
            </div>
            <div className="navbar__header">
                <a href="/">Beau's Collection</a>
            </div>
        </nav>
    )
}

export default Navbar;