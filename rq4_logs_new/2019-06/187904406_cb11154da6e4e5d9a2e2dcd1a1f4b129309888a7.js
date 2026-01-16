import React from "react"
import { Link } from "gatsby"

const Footer = () => {
    return (
        <footer>
            <Link to="/projekte/">Projekte | </Link>
            <Link to="/portfolio/">Portfolio | </Link>
            <Link to="/bildung/">Bildung</Link>
            {` `}

            oder sag hallo auf <a href="https://www.linkedin.com/in/paulanderie/">LinkedIn</a> {` `}
            {` `}
            {` `}
            {` `}
            {` `}
            und hier das {` `}<a href="https://prandible.com/impressum">Impressum</a>
        </footer>
    )
}

export  default Footer