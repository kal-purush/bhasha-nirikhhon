/**
 * APIごとのエンドポイント
 */
const API_ENDPOINT: PagePathProps = {
  GET_SERVER_INFO: '/get-minecraft-server-info',
}

type PagePathProps = {
  [PAGE: string]: string
}

export type ServerInfoProps = {
  isRunning: boolean
  onlinePlayers: number
  isFetched: boolean
}

export const API_KEY = process.env.REACT_APP_API_KEY

export const API_BASE_URL = process.env.REACT_APP_API_BASE_URL

export default API_ENDPOINT