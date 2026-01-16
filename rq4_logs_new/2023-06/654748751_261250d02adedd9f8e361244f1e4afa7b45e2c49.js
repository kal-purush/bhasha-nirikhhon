import axios from 'axios'
import React from 'react'

const filters = (email) => {
    getUser(email)
  return (
    <div>filters</div>
  )
}
const getUser =(email)=>{
    axios.get(`http://localhost:3004/users?email=${email}`)
    .then((data)=>{
        console.log(data,'mmmmmmm');
    })
    .catch((err)=>{
        console.log(err,'mmmmmmm');
    })
}
export default filters