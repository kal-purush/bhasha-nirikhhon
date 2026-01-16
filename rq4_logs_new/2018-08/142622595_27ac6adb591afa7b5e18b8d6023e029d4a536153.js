import React from 'react';
import { Parallax } from 'react-scroll-parallax';

export default class InfoContainer extends React.Component {
render() {
    const infoStyle = {
        position: 'relative',
        marginTop: '600px',
        width: '90%',
        height: '1200px',
        margin: 'auto',
        zIndex: 100,
        backgroundColor: '#000',
        top: '160px',
        }
    return (
        <Parallax
        className="custom-class"
        offsetYMax={20}
        offsetYMin={-20}
        slowerScrollRate
        tag="figure"
    >
        <div className='shadow p-3 mb-5 rounded' style={infoStyle}>
        </div>
        </Parallax>
    );
  }
}