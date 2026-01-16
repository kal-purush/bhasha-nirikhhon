import 'server-only'
import { db } from './db'
import { auth } from '@clerk/nextjs/server'

type Post = {
  id: number
  title: string
  image: string
  content: string
  createdAt: Date
  updatedAt: Date | null
}

export async function getPosts() {
  const posts: Post[] = await db.query.posts.findMany({
    orderBy: (model, { desc }) => desc(model.id)
  })
  return posts
}

// export async function getUserPosts(userId: number) {
//   const user = auth()

//   if (!user.userId) throw new Error('User not found')

//   const posts: Post[] = await db.query.posts.findMany({
//     where: (model, {eq}) => eq(model.userId, userId),
//     orderBy: (model, { desc }) => desc(model.id)
//   })
//   return posts
// }