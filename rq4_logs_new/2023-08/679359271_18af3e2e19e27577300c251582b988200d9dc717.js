import React from 'react'

const Details = () => {
  return (
    <div className='flex px-10 mt-5'>
    <img src='https://via.placeholder.com/200' alt='' />
    <div className='flex flex-col'>
    <h1>Movie Title (Rating)</h1>
    <div className='flex'>
    <span>Year</span>
    <span>Lenght</span>
    <span>Director</span>
    </div>
    <div className='flex'>
    <p>Cast: cast1, cast2, ...</p>
    </div>
    <div>
    <p>Description</p>
    </div>
    </div>
    </div>
  )
}

export default Details