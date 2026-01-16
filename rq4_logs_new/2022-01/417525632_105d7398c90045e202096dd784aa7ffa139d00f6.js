import React from 'react'
import {useHistory} from 'react-router-dom'
import classes from './thankyou.module.css'

const ThankYou = (props) =>{
    const history = useHistory();
    const homeHandler = () =>{
        history.push('/home')
        props.onHome(false)
    }
    return(
        <div className={classes.size}>
            <h1>Thank You for your order. It has been confirmed.</h1>
            <button className='btn-primary' onClick={homeHandler}>Go to Home</button>
        </div>
    )
}

export default ThankYou;