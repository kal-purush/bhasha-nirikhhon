import React from 'react'

export default function Clima({resultado}) {
  const {name, main} = resultado
  if (!name) return null

  const kelvin = 273.15



  return (
    <div className='card-panel white col s12'>
      <div className='black-text'>
        <h2>El clima de {name}</h2>
        <p className='temperatura'>{parseInt(main.temp - kelvin, 10)} <span>ºc</span></p>
        <p>temperatura Max: {parseInt(main.temp_max - kelvin, 10)} ºc</p>
        <p>temperatura Min: {parseInt(main.temp_min - kelvin, 10)} ºc</p>
      </div>
    </div>
  )
}