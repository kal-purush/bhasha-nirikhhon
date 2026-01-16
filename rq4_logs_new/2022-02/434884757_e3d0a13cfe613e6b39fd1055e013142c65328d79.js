import React from 'react';
import PropTypes from 'prop-types';
import classes from './Button.module.css';

Button.propTypes = {
    
};

function Button(props) {
    return (
        <button className={classes.button} type={props.type || 'button'}  onClick={props.onClick} >{props.children}</button>
    );
}

export default Button;