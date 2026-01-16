import React, { useState } from 'react'

export const ColorContext = React.createContext()

export function ColorContextProvider({ children }) {
    const [color, setColor] = useState('#35CE8D')

    const toggleColor = () => {
        color === '#35CE8D' ? setColor('#721817') : setColor('#35CE8D')
    }

    const value = { color, toggleColor }

    return <ColorContext.Provider value={value}>{children}</ColorContext.Provider>
}