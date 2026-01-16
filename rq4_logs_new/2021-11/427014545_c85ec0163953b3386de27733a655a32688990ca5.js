import React from 'react';
import Rentomojo from '../assets/images/logo.svg'
import TY from '../assets/images/ty.png'
import ITC from '../assets/images/itc.png'
const Experience = () => {

    return (
        <div className="skills" style={{ justifyContent: 'center', display: 'flex' }}>
            <div className="skills-container">
                <ul id="Experience">
                    <li className="transition2">
                        <div className="icon-contianer one">
                            <img src={Rentomojo} alt="Rentomojo" />
                        </div>
                        <p className="skill-title"> Rentomojo</p>
                        <p className="featured-desc skills-desc">Senior SDET</p>
                    </li>

                    <li className="transition2">
                        <div className="icon-contianer two">
                            <img src={TY} alt="Test Yantra" />
                        </div>
                        <p className="skill-title"> Test Yantra </p>
                        <p className="featured-desc skills-desc">Senior SDET</p>
                    </li>

                    <li className="transition2">
                        <div className="icon-contianer two">
                            <span class="skills-captera-title">CAPTERA</span>
                        </div>
                        <p className="skill-title"> Captera Solutions</p>
                        <p className="featured-desc skills-desc">QA Engineer</p>
                    </li>
                    <li className="transition2">
                        <div className="icon-contianer two">
                            <img src={ITC} alt="ITC" />
                        </div>
                        <p className="skill-title"> ITC Infotech</p>
                        <p className="featured-desc skills-desc">Software Engineer</p>
                    </li>
                </ul>
            </div>
        </div>

    )
}

export default Experience;