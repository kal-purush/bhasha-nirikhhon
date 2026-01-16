import React from 'react'
import logo from './images/hema.jpg';
import  './App.css';
import scroll from './images/scroll-top.png';
import p1 from './images/p1.png';
import p2 from './images/p2.png';
import { useState } from 'react';
import axios from 'axios';


const App = () => {
    const[value,setValue]=useState({
        name:"",
        email:"",
        project:"",
        message:""
    })
    const handler = async(e)=>{
        e.preventDefault();
        try{
        const {data} = await axios.post("http://localhost:7000/api/user",{
        ...value,
        })
    }catch(err){
        console.log(err);
    }

    }
    return (
    <div>
    <header>
<div className='user'>
<img src={logo} alt='profile'/>
<h3 className='name'>Hemnath</h3>
<p className='post'>Front End Developer</p>
</div>
<nav className='navbar'>
    <ul>
        <li><a href='#home'>Home</a></li>
        <li><a href='#about'>About</a></li>
        <li><a href='#education'>Education</a></li>
        <li><a href='#portfolio'>Project</a></li>
        <li><a href='#contact'>Contact</a></li>

    </ul>
</nav>

</header>

<div id='menu'><i class="bars icon"></i></div>

    


<section className='home' id='home'>
    <h3>HI THERE !</h3>
    <h1>I'M <span>Hemnath</span></h1>
    <p>Front End Developer</p>
    <a href='#about'><button className='btn'>About me <i class="user icon"></i></button></a>
</section>

<section className='about' id='about'>
<h1 className='heading'> <span>about</span>me</h1>
<div className='row'>
    <div className='info'>
        <h3><span>name:</span>Hemnath</h3>
        <h3><span>age:</span>20</h3>
        <h3><span>qualification:</span>BCA</h3>
        <h3><span>post:</span>Front end developer</h3>
        <h3><span>language:</span>English, tamil</h3>
        <a href='#'><button className='btn'>Download CV <i class="download icon"></i></button> </a>
    </div>
    <div className='counter'>
        <div className='box'>
            <span>2+</span>
            <h3>Year of Experience</h3>
        </div>

<div className='box'>
    <span>3</span>
    <h3>Project Completed</h3>
</div>

<div className='box'>
    <span>1</span>
    <h3>Happy Clients</h3>
</div>

<div className='box'> 
<span>Nil</span>
<h3>Awords won</h3>
</div>

    </div>

</div>

</section>

<section className='education' id='education'>
    <h1 className='heading'>My <span>Education</span></h1>
    <div className='box-container'>
        <div className='box'>
        <i class="graduation cap icon"></i>
            <span>2021</span>
            <h3>BCA</h3>
            <p>St.Joseph's College of Arts And Science (Autonomous) </p>
        </div>

<div className='box'>
<i class="graduation cap icon"></i>
    <span>2022</span>
    <h3>front end developer</h3>
    <p>free lenser</p>
</div>

<div className='box'>
<i class="graduation cap icon"></i>
    <span>2022</span>
    <h3>front end developer</h3>
    <p>free lenser</p>
</div>
<div className='box'>
<i class="graduation cap icon"></i>
    <span>2022</span>
    <h3>front end developer</h3>
    <p>free lenser</p>
</div>
<div className='box'>
<i class="graduation cap icon"></i>
    <span>2022</span>
    <h3>front end developer</h3>
    <p>free lenser</p>
</div>
<div className='box'>
<i class="graduation cap icon"></i>
    <span>2022</span>
    <h3>front end developer</h3>
    <p>free lenser</p>
</div>
    </div>
</section>

<section className='portfolio' id="portfolio">

<h1 className='heading'>My <span>Project</span></h1>
<div className='box-container'>
<div className='box'>
    <img src={p1} alt=''/>
</div>

<div className='box'>
    <img src={p2} alt=""/>
</div>

</div>

</section>

<section className='contact' id='contact'>

    <h1 className='heading'><span>Contact</span>me </h1>
    <div className='row'>
        <div className='content'>
            <h3 className='title'> Contact Info </h3>
            <div className='info'>
                <h3><i class="envelope icon"></i> hemnathfed@gmail.com</h3>
                <h3><i class="phone icon"></i> +916382735981</h3>
                <h3><i class="phone icon"></i>+917397050284</h3>
                <h3><i class="map marker alternate icon"></i> No.54, Mariyamman kovil st, Vanamadevi, Cuddalore - 607 105. </h3>
                </div> 
        </div>

<form action=''>
    <input type="text" placeholder='name' name='name' className='box' onChange={(e)=>setValue({...value,[e.target.name]:e.target.value})}/>
    <input type='email' placeholder='email' name='email' className='box' onChange={(e)=>setValue({...value,[e.target.name]:e.target.value})}/>
    <input type='text' placeholder='project' name='project' className='box' onChange={(e)=>setValue({...value,[e.target.name]:e.target.value})}/>
    <textarea name='message' id='' cols='30' row='10'  className='box message' placeholder='message' onChange={(e)=>setValue({...value,[e.target.name]:e.target.value})}/>
    <button type='submit' className='btn' onClick={handler}>send <i class="paper plane icon"></i></button>
</form>

    </div>
</section>
<a href='#home' className='top'>
    <img src={scroll} alt=''/>
</a>

</div>

    );
}

export default App;