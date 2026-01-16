import React, { useEffect, useState } from "react";
import io from "socket.io-client";
import Table from './Table'
import './App.css'
import { BASE_URL } from './constants'

function App() {
  const [pitches, setPitches] = useState([])
  const [socket, setSocket] = useState({})
  const [isConnected, setIsConnected] = useState(false)

  const handleUpdateEvent = (data) => {
    setPitches(prevPitches => {
      const newPitches = [...prevPitches]

      const idx = newPitches.findIndex(p => p.id === data.id)
      if (idx === -1) {
        newPitches.push(data)
      } else {
        newPitches.splice(idx, 1, data)
      }
      
      return newPitches
    })
  }

  useEffect(() => {
    fetch(`${BASE_URL}/pitches`)
      .then(res => res.json())
      .then(setPitches) 
  }, [])

  useEffect(() => {
    const s = io(BASE_URL)

    s.connect()
    s.on('connect', () => setIsConnected(true))
    s.on('create', handleUpdateEvent)
    s.on('update', handleUpdateEvent)
    s.on('disconnect', () => setIsConnected(false))
    setSocket(s)
  }, [])

  return (
    <div>
      <h2>
        {isConnected && "Connected"}
        {!isConnected && "Disconnected"}
      </h2>
      <Table pitches={pitches} />
    </div>
  );
}

export default App;