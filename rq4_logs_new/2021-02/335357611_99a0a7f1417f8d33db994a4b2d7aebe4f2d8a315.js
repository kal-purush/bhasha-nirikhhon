import React from 'react';

const Avatar = (props) => {
  return (
    <div>
      <img src={props.image} alt={props.altTxt} />
      <div>{props.person}</div>
      <div>{props.title}</div>
    </div>
  );
};

export default Avatar;