import React from 'react';

const Die = (props) => {
    return (
        <div onClick={props.onHold} className={props.isHeld ? 'die-face-held' : 'die-face'}>
            <h2 className='die-num'>{props.value}</h2>
        </div>
    );
};

export default Die;