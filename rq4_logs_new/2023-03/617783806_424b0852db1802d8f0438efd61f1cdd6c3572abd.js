import React from "react";
import Proptypes from 'prop-types'

export default function Navbar(props) {
  return (
    <>
      <nav className="navbar bg-body-tertiary">
        
        <div className="container-fluid">

          <a className="navbar-brand" href="/">
            <img src={props.brandSrc}
              alt=""
              width="60"
              className="d-inline-block align-text-top" />

            <div className="brand-name">{props.brandName}</div>

            {/* <div className="container-fluid">
              <form className="d-flex" role="search">
                <input
                  className="form-control me-2"
                  type="search"
                  placeholder={props.searchBox}
                  aria-label="Search"
                />
                <button className="btn btn-outline-success" type="submit">
                  Search
                </button>
              </form>
            </div> */}

          </a>

        </div>

      </nav>
    </>
  );
}

Navbar.propTypes = {
    brandSrc: Proptypes.string.isRequired,
    brandName: Proptypes.string.isRequired,
    searchBox: Proptypes.string.isRequired
}
Navbar.defaultProps = {
    brandSrc: 'Imgsrc Here',
    brandName: 'BrandName Here',
    searchBox: 'What to search for'
};