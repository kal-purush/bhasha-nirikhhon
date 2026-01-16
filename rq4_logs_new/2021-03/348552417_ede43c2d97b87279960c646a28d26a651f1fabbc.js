import React, { Component } from 'react';
import {Link } from 'react-router-dom';

class Footer extends Component {
    render() {
        return (
            <footer>
                <div className="github">
                    <a href="https://github.com/Trevorcdn" target="_blank" rel="noopener noreferrer" >
                        <img src={'/images/github.png'} style={{ textAlign: "center" }} />
                    </a>
                </div>
            </footer>
                
        );
    }
}

export default Footer;