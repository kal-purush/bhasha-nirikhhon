import React from "react";
import {Link} from 'react-router-dom'
import './Home.css';


const guide = () => {
    return (  
        <div>
            <h1><Link to='/' style={{ textDecoration: 'none', color: 'black'}}>Windows Guide</Link></h1>
                <video width='1000' controls>
                    <source src='https://placehold.co/1920x1080.mp4' type='video/mp4' />
                </video>
        </div>
    );
}
 
export default guide;