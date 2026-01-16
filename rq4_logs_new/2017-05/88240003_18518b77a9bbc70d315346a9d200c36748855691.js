import React from 'react';
import scrollToElement from 'scroll-to-element';
import { Link } from 'react-router-dom';

class NavBarContactUs extends React.Component {


  constructor(props) {
    super(props);
    this.handleContactUsClick = this.handleContactUsClick.bind(this);
  }

  handleContactUsClick(e){
    scrollToElement('.footerContainer',{
      offset: -60,
    });
  }

  render(){
    return (
    <header className="navbar navbar-fixed-top" id="top">
      <div className="container-fluid navBarContainer nav-wrapper change-color">
      <div className="navbar-header">
        <button aria-controls="bs-navbar" aria-expanded="false" className="navbar-toggle collapsed" data-target="#bs-navbar" data-toggle="collapse" type="button">
          <span className="sr-only">Toggle navigation</span>
          <span className="icon-bar"></span>
          <span className="icon-bar"></span>
          <span className="icon-bar"></span>
        </button>
        <Link to="/"><img src = './images/linq.jpg' className = 'img-responsive linqNavBarImage'/></Link>
      </div>
      <nav className="navbar-collapse collapse" id="bs-navbar" aria-expanded="false">
        <ul className="nav navbar-nav">
          <li><Link to="/"  className = 'navBarLinks'>Home</Link></li>
          <li><Link to="/partner-with-us-form"  className = 'navBarLinks'>partner with us</Link></li>
          <li><Link to="/open-a-store-form"  className = 'navBarLinks'>Open a Store</Link></li>
        </ul>
        <ul className="nav navbar-nav navbar-right">
          <li> <a href="http://linqstore.blogspot.in/" className = 'navBarLinks' target = "_blank">Blog</a> </li>
          <li><Link to="/gallery"  className = 'navBarLinks'>Gallery</Link></li>
          <li><Link to="/careers"  className = 'navBarLinks'>Careers</Link></li>
          <li><Link to="/contactUs"  className = 'navBarLinks'>Contact Us</Link></li>
        </ul>
      </nav>
      </div>
    </header>
    );
  }
}

export default NavBarContactUs;