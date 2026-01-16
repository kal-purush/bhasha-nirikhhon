import React from 'react'
import { Text, Image } from 'react-native'

function LoadStars (estrellas) {
  const nro_stars = estrellas.estrellas
  const stars = !isNaN(nro_stars) ? parseInt(nro_stars, 10) : 0
  const nulas = 5 - stars
  const coloreadas = []
  for (let i = 0; i < stars; i++) {
    coloreadas.push(<Image key={i} source= {require("../../images/star_full.png")} style={{height: 15, width:15}}/>)
  }

  const vacias = []
  for (let i = 0; i < nulas; i++) {
    vacias.push(<Image key={i} source= {require("../../images/star_empty.png")} style={{height: 15, width:15}}/>)
  }
  return (
        <Text>
          {coloreadas} {vacias}
        </Text>
  )
}

export {LoadStars}