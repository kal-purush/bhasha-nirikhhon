import React from 'react'


export default function Movie({ title, img }){
    return <div>
                <h2>watch {title} now</h2>
                <img src={img}></img>
            </div>
}