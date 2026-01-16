import React, { useEffect } from "react";
import webDev from "../assets/img/wevDev.jpg";
import js from "../assets/img/nodejs.jpg";
import reactjs from "../assets/img/reactjs.jpg";
import AOS from "aos";
import 'aos/dist/aos.css'

export const Blog = () => {
    useEffect(() => {
        AOS.init({duration: 2000})
    }, [])

    return (
        <div className='blog p-5' id='blog'>
            <div className='blogText'>
                <h3>Blog Article</h3>
                <br /><hr />
            </div> 
            <div className='container text-center mt-4'>
                <div className='row'>
                    <div className='col-12 col-md-6 col-lg-4 p-4' data-aos="flip-right">
                        <div className="card text-dark">
                            <img src={webDev} className="card-img-top" alt="UIUX" />
                            <div className="card-body">
                                <h5 className="card-title">Intro Web Development</h5>
                                <p className="card-text">Pengenalan Web Development seperti materi tentang Unix Command Line, Git & Github, HTML dan CSS</p>
                                <a href="https://github.com/devaaditya28/Writing-Presentation-Test-Skilvul/blob/master/Week-1/Week1-Writing-Test.md" className="btn">More Detail</a>
                            </div>
                        </div>
                    </div>
                    <div className='col-12 col-md-6 col-lg-4 p-4' data-aos="zoom-in">
                        <div className="card text-dark">
                            <img src={js} className="card-img-top" alt="Web Dev" />
                            <div className="card-body">
                                <h5 className="card-title">Intro Javascript</h5>
                                <p className="card-text">Pengenalan materi Javascript seperti scope & function, error reference, property method dan DOM</p>
                                <a href="https://github.com/devaaditya28/Writing-Presentation-Test-Skilvul/blob/master/Week-2/Week2-Writing-Test.md" className="btn">More Detail</a>
                            </div>
                        </div>
                    </div>
                    <div className='col-12 col-md-6 col-lg-4 p-4' data-aos="flip-left">
                        <div className="card text-dark">
                            <img src={reactjs} className="card-img-top" alt="CV" />
                            <div className="card-body">
                                <h5 className="card-title">Intro ReactJs</h5>
                                <p className="card-text">Pengenalan materi ReactJs mulai dari pengertian, keunggulan, instalasi, jsx, react component dan event handler</p>
                                <a href="https://github.com/devaaditya28/Writing-Presentation-Test-Skilvul/blob/master/Week-6/Week6-Writing-Test.md" className="btn">More Detail</a>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}

export default Blog;