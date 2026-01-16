import React from 'react';
import { BrowserRouter as Router, Link } from 'react-router-dom';
import './Footer.css';

const Footer = () => {
    const year = new Date().getFullYear();

    return (
        <Router>
            <footer className="footer">
                <div className="footer-links">
                    <Link to="#">Políticas de privacidad</Link>
                    <Link to="#">Condiciones de uso</Link>
                    <Link to="#">Defensa del consumidor</Link>
                    <Link to="#">Ayuda</Link>
                </div>
                <div className="footer-content">

                    <div className="footer-text">
                        <p className='footer-text-copy'>© {year} AX SHOP. Todos los derechos reservados.</p>
                    </div>
                </div>
            </footer>
        </Router>
    );
};

export default Footer;