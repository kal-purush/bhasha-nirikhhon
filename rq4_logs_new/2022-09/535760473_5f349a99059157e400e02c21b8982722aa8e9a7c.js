import React from 'react';
import './Blog.css'
const Blog = () => {
    return (
        <div>
            <div className='mx-auto text-center my-10'>
                <h1 className='my-5 text-2xl font-serif text-fuchsia-400 '>This is all about <span className='text-orange-500 font-bold'>My-self</span></h1>

                <div>
                    <div className="indicator w-32">
                        <img src="https://i.ibb.co/mv2HKDG/portflio-removebg-preview.png" />
                    </div>

                    <div className="card text-justify w-96 bg-base-100 shadow-xl mx-auto text-center my-2">
                        <div className="card-body">
                            <h2 className="card-title">Name: <span className='text-purple-500'>Sabbir Hossen</span></h2>
                            <h2>Email: <spna className='text-purple-500'>sabbir.ms2000@gmail.com</spna></h2>
                            <h3>Educational Background:<br></br>
                                <span className='text-orange-600'>Studies at B.S.C in honours life Department of Physics</span></h3>
                            <p className='text-green-500 font-semibold'>List of Technology skills: <br></br>
                                <span className='font-bold font-mono'>
                                    <ul className=' text-purple-600 my-2'>
                                        <li>Html5</li>
                                        <li>Css3</li>
                                        <li>Bootstrap</li>
                                        <li>Tailwind</li>
                                        <li>Javascript</li>
                                        <li>ReactJS</li>
                                        <li>NodeJS</li>
                                        <li>ExpressJs</li>
                                    </ul>
                                    <h2 className='text-2xl text-red-500'>React.Js Developer</h2>
                                    <h2 className='text-2xl  text-red-500'>Front End Developer</h2>

                                </span></p>

                        </div>
                    </div>

                </div>
                <div className="mockup-code w-32 mx-auto text-center">
                    <h2 className='text-1xl font-bold text-purple-400'>My React project Link</h2>
                    <li> <a href='https://assingment-9-chair-project-163571.netlify.app/'>Chair-Analysis</a></li>
                    <li> <a href='https://wonder-of-books-house.web.app/'>Book-House</a></li>
                    <li>  <a href='https://teach-pitch-project.web.app/'> Teach</a></li>

                </div>
            </div>
        </div>
    );
};

export default Blog;