import React from 'react';
import './AdvProducts.css';
import drone3 from '../assets/img/drone3.webp';
import dron4 from '../assets/img/dron4.webp';
import VXE from '../assets/img/VXE-w-FPV-Havoc-qnrouhl5k1d6vhtg7k08a9025epf9hztqwzhia7hb6.jpg';
import fronTop from '../assets/img/05-Fron-top-1-White-Edge_PenguinC-768x432.webp';


const AdvProducts = () => {
    return (
        <div>
            <div className="container ">
                <div className="row ">
                    <div className="col-md-6">
                        <img src={drone3} alt="" className='img-fluid'/>
                    </div>
                    <div className="col-md-6 d-flex">
                        <div className='align-content-center flex-wrap'>
                            <h2>VXE30 Stalker</h2>
                            <p>This small, uncrewed aircraft system (SUAS) features quick and easy assembly and silent operations for effective long-range reconnaissance (LRR).</p>
                            <a href="/" className='text-decoration-none'>LEARN MORE ↓</a>
                        </div>
                    </div>
                </div>
                <hr />


                <div className="row py-5">
                    
                    <div className="col-md-6 d-flex">
                        <div className='align-content-center flex-wrap'>
                            <h2>VXE30 STALKER HAVOC CONFIGURATION</h2>
                            <p>With a major performance enhancement to the field-proven VXE30 Stalker UAS through a series of subsystem upgrades we haves doubled the flight endurance and payload capacity of the base VXE30 Stalker system, closing the gap between the capabilities of small UAS and large UAS.</p>
                            <a href="/" className='text-decoration-none'>LEARN MORE ↓</a>
                        </div>
                    </div>

                    <div className="col-md-6">
                        <img src={VXE} alt="" className='img-fluid'/>
                    </div>
                </div>
                <hr />


                <div className="row ">
                    <div className="col-md-6">
                        <img src={dron4} alt="" className='img-fluid'/>
                    </div>
                    <div className="col-md-6 d-flex">
                        <div className='align-content-center flex-wrap'>
                            <h2>PENGUIN C VTOL</h2>
                            <p>Rapidly deployable for critical mission support, the Penguin C VTOL has the long endurance and long range to address multi-mission operations</p>
                            <a href="/" className='text-decoration-none'>LEARN MORE ↓</a>
                        </div>
                    </div>
                </div>
                <hr />


                <div className="row py-5">
                    
                    <div className="col-md-6 d-flex">
                        <div className='align-content-center flex-wrap'>
                            <h2>PENGUIN C MK2</h2>
                            <p>This long endurance, long range UAS has an endurance of up to 25 hours (payload dependent) and a line-of-sight range of 180 km to meet a wide variety of mission needs.</p>
                            <a href="/" className='text-decoration-none'>LEARN MORE ↓</a>
                        </div>
                    </div>

                    <div className="col-md-6">
                        <img src={fronTop} alt="" className='img-fluid'/>
                    </div>
                </div>
                <hr />



            </div>
        </div>
    );
};

export default AdvProducts;