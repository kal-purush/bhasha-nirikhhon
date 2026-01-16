import { Component } from "react";
import './FooterAside.css';

class FooterAside extends Component  {
  render() {
    return (
       <footer id="footer">
        <div className="container">
          <div className="text-center">
            &copy; Copyright <strong>Gabriel Santos</strong>
          </div>
        </div>
      </footer>
    );
  }
}

export default FooterAside;