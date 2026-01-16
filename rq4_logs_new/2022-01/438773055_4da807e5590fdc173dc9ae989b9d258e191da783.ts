import { getIo } from '../../socket/io'

type RoomObject = {
  [key: string]: string[] | string
}
export const getAllRooms = (): RoomObject => {
  const io = getIo()
  const { rooms } = io.sockets.adapter

  const iterableRooms = [...rooms.entries()]

  const userRooms = iterableRooms.filter((ab) => {
    return ab[0][0] === '#'
  })

  const obj: RoomObject = {}
  userRooms.forEach((room) => {
    const roomName = room[0]
    const users = [...room[1]]
    obj[roomName] = users.map((id) => {
      return io.sockets.sockets.get(id)?.data.username
    })
  })

  return obj
}