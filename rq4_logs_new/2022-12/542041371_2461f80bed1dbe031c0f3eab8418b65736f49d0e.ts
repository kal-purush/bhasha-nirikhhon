import { ForumBaseAPI } from './base';
import {
  ForumCommentDTO,
  Forum,
  ForumUserDTO,
  PostDTO,
  ThreadDTO,
  LikeDislike,
  UserId,
  CreateEntity,
} from '../types/forum';
import { ApiResponse } from '../types/api';
import { UserDTO } from '../types/user';

class ForumAPI extends ForumBaseAPI {
  constructor() {
    super('/forum');
  }

  getForum(): ApiResponse<Forum> {
    return this.httpService.get('', {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  getThreads(): ApiResponse<ThreadDTO[]> {
    return this.httpService.get('/thread', {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  createThread(body: CreateEntity): ApiResponse<ThreadDTO> {
    return this.httpService.post('/thread', {
      body,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  getPosts(): ApiResponse<PostDTO[]> {
    return this.httpService.get('/post', {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  createPost(body: CreateEntity): ApiResponse<PostDTO> {
    return this.httpService.post('/post', {
      body,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  getComments(): ApiResponse<ForumCommentDTO[]> {
    return this.httpService.get('/comment', {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  createComment(body: CreateEntity): ApiResponse<ForumCommentDTO> {
    return this.httpService.post('/comment', {
      body,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  getUsers(): ApiResponse<ForumUserDTO[]> {
    return this.httpService.get('/user', {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  signIn(body: UserDTO): ApiResponse<ForumUserDTO> {
    return this.httpService.post('/user/signin', {
      body,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  setLike(
    body: UserId & LikeDislike
  ): ApiResponse<Partial<LikeDislike> & UserId> {
    return this.httpService.post('/like', {
      body,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  setDislike(body: UserId & LikeDislike): ApiResponse<LikeDislike & UserId> {
    return this.httpService.post('/dislike', {
      body,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

const forumAPI = new ForumAPI();

export default forumAPI;