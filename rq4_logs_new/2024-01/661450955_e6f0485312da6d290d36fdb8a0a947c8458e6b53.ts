import { FastifyReply, FastifyRequest } from 'fastify'
import { prisma } from '../../database/prisma'
import { boolean, string, z } from 'zod'

class PostControllers {
  async getPosts() {
    const posts = await prisma.post.findMany({
      orderBy: {
        content: 'asc',
      },
      select: {
        id: true,
        title: true,
        authorId: true,
        content: true,
        createdAt: true,
        published: true,
      },
    })
    return posts
  }

  async postPost(request: FastifyRequest, reply: FastifyReply) {
    const bodySchema = z.object({
      title: string(),
      content: string(),
      published: boolean(),
      authorId: string(),
    })

    const { authorId, published, content, title } = bodySchema.parse(
      request.body,
    )

    const seachUser = await prisma.user.findUnique({
      where: {
        id: authorId,
      },
    })
    console.log(seachUser)
    if (!seachUser) {
      return reply.status(401).send()
    }
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

  async putPost(request: FastifyRequest, reply: FastifyReply) {
    const bodySchema = z.object({
      id: string(),
      title: string(),
      content: string(),
      published: boolean(),
    })
    const paramsSchema = z.object({
      userId: string(),
    })

    const { userId } = paramsSchema.parse(request.params)
    const { id, title, content, published } = bodySchema.parse(request.body)

    console.log(userId)

    const seachPost = await prisma.post.findUnique({
      where: {
        id,
      },
    })

    if (!seachPost) {
      return reply.status(404).send()
    }

    if (userId !== seachPost.authorId) {
      return reply.status(403).send()
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

  async deletePort(request: FastifyRequest, reply: FastifyReply) {
    const bodySchema = z.object({
      id: string(),
    })
    const paramsSchema = z.object({
      userId: string(),
    })

    const { id } = bodySchema.parse(request.body)
    const { userId } = paramsSchema.parse(request.params)

    const seachPost = await prisma.post.findUnique({
      where: {
        id,
      },
    })
    if (!seachPost) {
      return reply.status(404).send()
    }
    if (seachPost.authorId !== userId) {
      return reply.status(403).send()
    }

    const deletePost = await prisma.post.delete({
      where: {
        id,
      },
    })
    return deletePost
  }
}
export { PostControllers }