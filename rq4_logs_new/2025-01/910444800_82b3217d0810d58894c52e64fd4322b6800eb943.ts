import type { Context } from 'hono';
import type { IPostUsecase } from '../../application/usecases/PostUsecase';
import { createPostId } from '../../domain/models/Post';

export class PostController {
  constructor(private readonly postUsecase: IPostUsecase) {}

  async getPost(c: Context) {
    const id = Number.parseInt(c.req.param('id'));
    const postId = createPostId(id);
    const post = await this.postUsecase.getPost(postId);
    return c.json(post);
  }

  async getAllPosts(c: Context) {
    const posts = await this.postUsecase.getAllPosts();
    return c.json(posts);
  }

  async createPost(c: Context) {
    const request = await c.req.json();
    const message = await this.postUsecase.createPost(request);
    return c.json(message);
  }
}