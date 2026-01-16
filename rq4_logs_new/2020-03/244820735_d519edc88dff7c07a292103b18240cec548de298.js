import React from 'react';
import PropTypes from 'prop-types';
import './index.css';

const Button2 = ({ text }) => <button className="btn">这是一个组件按钮2{text}</button>

Button2.propTypes = {
  text: PropTypes.any
};

export default Button2;