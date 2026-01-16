import React from "react"
import { Link } from "react-router-dom";
import './Navi.css'


function Navi(){

    return (
        <div className="nav">
            <Link to="/" >HOME</Link>
            <Link to="/about">ABOUT</Link>
        </div>
    );
}
export default Navi;