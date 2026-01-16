import { useEffect, useState } from 'react';
import './notification.css';

export default function Notification({type, message, onClose, status}){
    const [displayNotification, setDisplayNotification] = useState(status);

    useEffect(()=>{
        setTimeout(function (){
            setDisplayNotification(false);
        },1000);
    },[displayNotification])
    return(
        <div className={`Message ${type}`} id="js-timer">
            {displayNotification &&
                <div className="Message-body">
                    <p>{message}</p>
                </div>            
            }
        </div>
    )
}