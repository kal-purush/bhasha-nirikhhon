import React, {useState} from 'react';
import Modal from "./Modal";

const Footer = () => {
    const [modal, setModal] = useState(false)

    const toggle = () => {
        setModal(!modal);
    }
    return (
        <>
            <footer className='footer'>
                <button className='rules' onClick={toggle}> Rules</button>
            </footer>
            {
                modal?
                    <Modal toggle={toggle}/> : null
            }
        </>

    );
};

export default Footer;