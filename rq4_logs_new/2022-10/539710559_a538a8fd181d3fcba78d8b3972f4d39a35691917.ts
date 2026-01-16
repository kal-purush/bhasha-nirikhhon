import { createContext } from 'react'

export interface Events {
  bbq?: boolean
  civil?: boolean
  mehndi?: boolean
  reception?: boolean
}

export const EventsContext = createContext<Events>({})