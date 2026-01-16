import { Socket } from 'socket.io'

type PlaylistEventData = {
  type: 'next' | 'previous'
  room: string
  item?: {
    url: string
    title: string
    _id: string
  }
  position: number
}

export function onPlaylistChange(this: Socket, data: PlaylistEventData): void {
  console.log('playlistChange', data)
  const { type, room } = data

  switch (type) {
    case 'next':
      this.broadcast.to(room).emit('nextVideo')
      break
    case 'previous':
      this.broadcast.to(room).emit('previousVideo')
      break
    default:
      break
  }
}