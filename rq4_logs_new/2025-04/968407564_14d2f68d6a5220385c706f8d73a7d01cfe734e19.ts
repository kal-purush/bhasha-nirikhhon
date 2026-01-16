import type { RoomJoin, RoomUpdateNewMember } from "@sendhelp/core";
import db from "../db";

export async function roomJoin(
  body: RoomJoin,
  username: string,
): Promise<RoomUpdateNewMember | null> {
  const { roomName } = body;
  const user = db.data.users.find((user) => user.username === username);

  const room = db.data.rooms.find((room) => room.name === roomName);

  if (!user || !room) {
    return null;
  }

  room.members.push(user);

  await db.write();

  return {
    roomName,
    user,
  };
}