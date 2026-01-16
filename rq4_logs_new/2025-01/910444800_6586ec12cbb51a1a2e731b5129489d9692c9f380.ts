import type { Context } from 'hono';
import { inject, injectable } from 'inversify';
import type {
  CreatePostUseCase,
  CreatePostUseCaseInput,
} from '../../application/usecases/post/create-post.usecase';
import type { Post } from '../../domain/models/Post';
import { USECASE_BINDINGS } from '../../keys';
import type { BaseController } from './base.controller';

@injectable()
export class CreatePostController implements BaseController {
  constructor(
    @inject(USECASE_BINDINGS.CreatePostUseCase) private usecase: CreatePostUseCase,
  ) {}

  async main(c: Context) {
    return this.mainFn(c);
  }

  private async mainFn(c: Context) {
    const request = await c.req.json<Post>();
    const input: CreatePostUseCaseInput = {
      title: request.title,
      body: request.body,
    };
    const message = await this.usecase.execute(input);
    return c.json(message);
  }
}