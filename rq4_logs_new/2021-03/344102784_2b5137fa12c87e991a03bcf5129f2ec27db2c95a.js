import { createContext } from 'react'
import PropTypes from 'prop-types'

export const HatContext = createContext()

export const HatProvider = ({ children }) => {
  return <HatContext.Provider>{children}</HatContext.Provider>
}

HatProvider.propTypes = {
  children: PropTypes.node.isRequired
}