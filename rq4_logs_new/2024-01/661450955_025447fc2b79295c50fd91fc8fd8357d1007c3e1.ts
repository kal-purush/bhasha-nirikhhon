import { FastifyReply, FastifyRequest } from 'fastify'
import { PostServices } from '../services/postServise'
import {
  postSchemaId,
  postSchemaPost,
  postSchemaPut,
  schemaUserId,
} from './postSchemas'

export class PostController {
  constructor(private readonly postService: PostServices) {}

  async getPosts(request: FastifyRequest, reply: FastifyReply) {
    const post = await this.postService.getPost()

    return reply.status(200).send(post)
  }

  async createPost(request: FastifyRequest, reply: FastifyReply) {
    const { authorId, published, content, title } = postSchemaPost.parse(
      request.body,
    )
    const postService = new PostServices()

    const post = await postService.createPost({
      title,
      authorId,
      content,
      published,
    })

    return reply.status(201).send(post)
  }

  async updatePost(request: FastifyRequest, reply: FastifyReply) {
    const { userId } = schemaUserId.parse(request.params)
    const { id, title, content, published } = postSchemaPut.parse(request.body)

    const postService = new PostServices()

    const updatePost = await postService.updatePost({
      id,
      title,
      content,
      published,
      userId,
    })

    return reply.status(200).send(updatePost)
  }

  async deletePost(request: FastifyRequest, reply: FastifyReply) {
    const { id } = postSchemaId.parse(request.body)
    const { userId } = schemaUserId.parse(request.params)

    const deletePostServise = new PostServices()

    await deletePostServise.deletePost({ id, userId })

    return reply.status(200).send({ message: 'Post deleted with success' })
  }
}