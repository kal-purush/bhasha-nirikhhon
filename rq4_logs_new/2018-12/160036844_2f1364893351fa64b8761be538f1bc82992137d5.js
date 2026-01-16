import React, { Component } from 'react'
import { Link } from 'gatsby'

import styles from './footer.module.css'


class Footer extends Component {
    render () {
        return (
            <div className={styles.footerWrapper}>
                    <ul>
                        <li><Link to="/">Home</Link></li>
                        <li><Link to="/projects">Projects</Link></li>
                        <li><Link to="/about">CV</Link></li>
                        <li><Link to="/contact">Contact me</Link></li>
                    </ul>
            </div>
            )
    }
}

export default Footer