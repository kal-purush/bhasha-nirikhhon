import { createContext } from 'react'
import MockDataHelper from '../helpers/mockData.helper'

export const defaultContext = MockDataHelper.getEventsTitle()

const Context = createContext({
  context: defaultContext,
  setContext: (arg: string[]) => {},
})

export default Context