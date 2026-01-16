import { db } from './db'

export interface AppState {
  _id: string
  withClock: boolean
}

export async function setAppState(newState: Partial<AppState>): Promise<AppState | null> {
  try {
    const existingState = await getAppState()
    let updatedState

    if (existingState) {
      updatedState = {
        ...existingState,
        ...newState
      }
    } else {
      updatedState = {
        ...newState
      }
    }

    await db.put({
      ...updatedState,
      _id: 'app_state',
      type: 'state'
    })

    return updatedState
  } catch (error) {
    console.error('Error updating app state:', error)
    return null
  }
}

export async function getAppState(): Promise<AppState | null> {
  try {
    const doc = await db.get<AppState>('app_state')
    return doc
  } catch (error) {
    console.error('Error fetching app state:', error)
    return null
  }
}