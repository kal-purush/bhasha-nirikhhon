import React from 'react';

export default function SmallButton(props){
    return (
        <>
            <button className={`px-3 py-2 ${props.bg_color} rounded-full ${props.text_color} font-bold`}>{props.button_text}</button>
        </>
    ) 
}