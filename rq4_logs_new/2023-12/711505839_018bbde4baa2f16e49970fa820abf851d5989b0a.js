import React from 'react';
import { getPrimaryProps, getSecondaryProps } from './button.properties';

const PRIMARY = 'primary';
const SECONDARY = 'secondary';

let buttonStyle;

const Button = ({buttonType, onClick, content, size="16px", backgroundColor="#1b72e8", color="black", className}) => {
  switch (buttonType) {
    case PRIMARY:
     buttonStyle = getPrimaryProps(backgroundColor);
     break;
    case SECONDARY:
      buttonStyle = getSecondaryProps(size, color);
      break;
    default:
      buttonStyle = {
        padding: "0.5rem 1rem",
        backgroundColor: "#1b72e8",
        color: "white",
        border: "none",
        cursor: "pointer"
      }
  }
  return (
    <button className={className} style={buttonStyle} onClick={onClick}>{content}</button>
  )
};

export default Button;