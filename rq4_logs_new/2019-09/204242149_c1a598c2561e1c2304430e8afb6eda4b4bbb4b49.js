import React from 'react'
import {Redirect} from 'react-router-dom'


const withSession = Component => props => {

	const token = localStorage.getItem("token");

	if (!token) return <Redirect to={"/login"}/>

	return <Component {...props} />

}

export default withSession