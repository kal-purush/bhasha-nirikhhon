import React from 'react'
import './Popup.css'

function Popup(props) {
    return (props.trigger) ? ( 
        <div className='popup'>
            <div className='card card-tertiary'>
                <div className='card-header'>
                <div className="header-text">admission</div>
                </div>
                { props.children }
                {/* <button className='pbtn pbtn-primary' onClick={() => props.setTrigger(false)}>OK</button> */}
            </div>

        </div>
    ) : "";
}

export default Popup