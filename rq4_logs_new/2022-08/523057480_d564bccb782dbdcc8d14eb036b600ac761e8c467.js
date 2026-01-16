import React, { useState } from 'react'
import styles from './App.module.css'
import image from './assets/images/img-not-found.jpg'
import Card from './components/Card/Card'
import NavBar from './components/NavBar/NavBar'

import data from './data.json'

function App() {
  const [infoCards, setInfoCards] = useState(data.cards)
  const [input, setInput] = useState('') 

  console.log(infoCards[0].title)

  const getRangeValue = e => {
    const value = e.target.value
    console.log(value)
  }

  const changeCardTitle = (title, value) => {
    console.log(title, value)
    
  }

  // changeCardTitle('test', 6)
  return (
    <div className={styles.container}>
      <NavBar infoCardsLength={infoCards.length} algo={changeCardTitle} />
      {infoCards.map((item, index) => (
        <Card
          key={index}
          title={item.title}
          price={item.price}
          description={item.description}
          image={image}
          index={index}
        />
      ))}
      {/* <div>
        <label htmlFor='points'>Points (between 0 and 10):</label>
        <input type='range' id='points' name='points' min='0' max='10' onChange={getRangeValue} />
      </div> */}
    </div>
  )
}

export default App