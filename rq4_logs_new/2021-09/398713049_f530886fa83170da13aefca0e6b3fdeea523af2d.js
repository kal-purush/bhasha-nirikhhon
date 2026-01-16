import React from 'react';
import './Pallette.css'

const Color =({color,active,onClick})=>{
    return(
        <div className={"color ${active && 'active'}"} style={{background:color}} onClick={onClick}>
        </div>
    )
}

const Pallette=({colors,selected,onSelect})=>{
    const colorList = colors.map(
        (color)=>(<Color color={color} active={selected===color} onClick={()=>onSelect(color)} key={color}/>)
    );

    return(
        <div className='pallette'>
            {colorList}
        </div>
    );
};

export default Pallette;