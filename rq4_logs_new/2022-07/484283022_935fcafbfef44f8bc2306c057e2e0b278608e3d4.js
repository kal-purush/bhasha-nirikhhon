import React, { useEffect, useRef } from "react";
import $ from 'jquery';


export const CardInfo = ({icon, message, info, onSelect}) =>{
    const overlayContainerRef = useRef();
    const overlayRef = useRef();

    useEffect(()=>{
        $(overlayContainerRef.current).hover((e)=>{
            if(e.type === 'mouseenter'){
                onSelect(info);
                console.log($(e.currentTarget).parent()[0]);
                $(e.currentTarget).parent().find('[data-card]').removeClass('card-selected');
                $(e.currentTarget).addClass('card-selected');
            }
        });
    }, []);
    return(
        <>
        <div ref={overlayContainerRef} data-card>
            <div className="d-flex align-items-center">
                <div className="p-4" style={{width: '20%'}} data-icon-img>{icon}</div>
                <div className="p-4" style={{width: '100%'}}>{message}</div>
            </div>
        </div>
        <span className="text-dark show-on-mobile text-center m-auto">{info}</span>
        </>
    )
}