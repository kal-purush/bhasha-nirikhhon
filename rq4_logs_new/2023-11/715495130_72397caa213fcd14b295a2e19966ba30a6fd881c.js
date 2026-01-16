import React from 'react'
import { useNavigate } from 'react-router-dom'

const Header = ({Component, compName}) => {
    console.log({compName})
    const navigate = useNavigate()
    return (
        <>
        <div onClick={() => navigate('/profile')}>Profile</div>
        {Component && <Component />}
        <div>Footer</div>
        </>
    )
}

export default Header