import { io } from 'socket.io-client'

const gameSocketUrl = process.env.NEXT_PUBLIC_API_GAME_SOCKET_ENDPOINT
export let gameSocket = io(gameSocketUrl, {
  autoConnect: false,
  // reconnection: true,
  withCredentials: true,
})