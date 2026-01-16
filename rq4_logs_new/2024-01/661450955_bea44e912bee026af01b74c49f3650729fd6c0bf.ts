import { Post } from '@prisma/client'
import { prisma } from '../database/prisma'
import { CallError } from '../helpers/callError'

type deletePost = {
  id: string
  userId: string
}
type updatePost = {
  id: string
  title: string
  content: string
  published: boolean
  userId: string
}
type CreatePost = {
  title: string
  authorId: string
  content: string
  published: boolean
}
export class PostServices {
  async createPost({ title, authorId, content, published }: CreatePost) {
    const createPost = await prisma.post.create({
      data: {
        title,
        authorId,
        content,
        published,
      },
    })

    return createPost
  }

  async deletePost({ id, userId }: deletePost): Promise<void> {
    const seachPost = await prisma.post.findUnique({
      where: {
        id,
      },
    })

    if (!seachPost) {
      throw new CallError('Post not found', 404)
    }
    if (seachPost.authorId !== userId) {
      throw new CallError('User not Authorized', 403)
    }

    await prisma.post.delete({
      where: {
        id,
      },
    })
  }

  async getPost(): Promise<Post[]> {
    const posts = await prisma.post.findMany()

    return posts
  }

  async updatePost({
    id,
    title,
    content,
    published,
    userId,
  }: updatePost): Promise<Post> {
    const seachPost = await prisma.post.findUnique({
      where: {
        id,
      },
    })

    if (!seachPost) {
      throw new CallError('Post not found', 404)
    }
    if (userId !== seachPost.authorId) {
      throw new CallError('User not Authorized', 403)
    }

    const updatePost = await prisma.post.update({
      where: {
        id,
      },
      data: {
        title,
        content,
        published,
      },
    })
    return updatePost
  }
}