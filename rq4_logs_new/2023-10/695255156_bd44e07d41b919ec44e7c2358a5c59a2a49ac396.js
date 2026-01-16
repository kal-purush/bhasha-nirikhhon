import React from 'react'
import { Outlet } from 'react-router-dom'

const About = () => {
  return (
    <><div className="bg-gray-100 min-h-screen flex items-center justify-center">
      <div className="bg-white p-8 rounded shadow-md">
        <h1 className="text-3xl font-bold mb-4">About Us</h1>
        <p className="text-gray-600">
          Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam eget
          libero vel purus fringilla mattis.
        </p>
      </div>
    </div><Outlet /></>
  )
}

export default About