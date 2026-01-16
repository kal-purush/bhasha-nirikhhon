import React from "react";

export default function Button(props) {
    let Class;
    if (props.icon === 'endIcon') {
        Class = `bttn--endIcon`;
    }
    return (
        <button type={props.type} className={props.className}>Search</button>
    )
}