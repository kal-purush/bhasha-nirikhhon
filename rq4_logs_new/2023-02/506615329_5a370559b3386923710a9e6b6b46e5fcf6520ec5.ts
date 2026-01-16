import { ISpaces } from "components/Task/ImportTask/types"
import { setCookies } from 'cookies-next'
import { TOKEN_KEY } from './'

const serverUrl = process.env.NEXT_PUBLIC_CLICKUP_SERVER_URL
const client_secret = process.env.NEXT_PUBLIC_CLICKUP_CLIENT_SECRET
const client_id = process.env.NEXT_PUBLIC_CLICKUP_CLIENT_ID
const CLIENT_CODE = 'clickupClientCode'

export const fetchClickupTask = async (clickupTaskId: string, token: string) => {
  try {
    const response = await fetch(`${serverUrl}/task`, {
      method: 'POST',
      body: JSON.stringify({
        task_id: clickupTaskId,
        token: token
      }),
      headers: new Headers({ 'Content-Type': 'application/json' })
    })
    const clickupTask = await response.json()
    return clickupTask
  } catch (err) {
    console.error(err)
  }
}

export const fetchSpaces = async (token: string): Promise<ISpaces[]> => {
  let spaces
  try {
    const res = await fetch(`${serverUrl}/spaces`, {
      method: 'POST',
      body: JSON.stringify({
        token
      }),
      headers: new Headers({ 'Content-Type': 'application/json' })
    })
    const data = await res.json()
    spaces = data.spaces
  } catch (err) {
    console.error(err)
  }
  return spaces
}

export const fetchTasks = async (spaceId: number, token: string): Promise<any[]> => {
  let tasks
  try {
    const resp = await fetch(`${serverUrl}/tasks`, {
      method: 'POST',
      body: JSON.stringify({
        space_id: spaceId,
        token
      }),
      headers: new Headers({ 'Content-Type': 'application/json' })
    })
    const data = await resp.json()
    tasks = data
  } catch (err) {
    console.error(err)
  }
  return tasks
}

export const fetchToken = async (code: string) => {
  try {
    const res = await fetch(`${serverUrl}/get_token`, {
      method: 'POST',
      body: JSON.stringify({
        client_id,
        client_secret,
        code: code
      }),
      headers: new Headers({ 'Content-Type': 'application/json' })
    })
    const data = await res.json()
    if (data.access_token) {
      setCookies(TOKEN_KEY, data.access_token)
      setCookies(CLIENT_CODE, code)
      return data.access_token
    }
  } catch (e) {
    console.log('ERRORRR===', e)
  }
}