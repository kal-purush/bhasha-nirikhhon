import base64url from "base64url"
const defaultJoinDate = "1970-01-01T00:00:00Z"

// Encode cursor
export const encodeCursor = (userId: string | null, joinDate: Date): string => {
  const encodedJoinDate = joinDate ? joinDate.toISOString() : defaultJoinDate
  return base64url.encode(`${userId}|${encodedJoinDate}`)
}

// Decode cursor
export const decodeCursor = (
  cursor: string
): { userId: string; joinDate: Date } => {
  const [encodedUserId, encodedJoinDate] = base64url.decode(cursor).split("|")
  let joinDate = new Date(defaultJoinDate)
  if (encodedJoinDate !== defaultJoinDate) {
    joinDate = new Date(encodedJoinDate)
  }
  return { userId: encodedUserId, joinDate }
}