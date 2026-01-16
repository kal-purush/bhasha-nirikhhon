import React from "react";
import "./Navigation.css"
import { Link } from "react-router-dom";

function Navigation(){
    return (
        <div className="nav">
            <Link to="/">home</Link>
            <Link to="/about">about</Link>
        </div>
    )
}

export default Navigation;