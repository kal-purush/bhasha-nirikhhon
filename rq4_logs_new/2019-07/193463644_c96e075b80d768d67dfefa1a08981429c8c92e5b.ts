import { PostController } from '../controllers/post-controller';
import { PostService } from '../service/post-service';
import { mock, instance, spy, when, verify, deepEqual } from 'ts-mockito';
import { Repository, DeleteResult } from 'typeorm';
import { User } from '../entities/user';
import { UserService } from '../service/user-service';
import { Dependencies } from '../types';
import { Request, Response, ErrorRequestHandler } from 'express-serve-static-core';
import { Post } from '../entities/post';
import { validateParams, validatePost, validatePostPUT } from '../middleware/validation';
import { NextFunction } from 'connect';

const postMockRepository = mock(Repository) as Repository<Post>;
const postMockInstance = instance(postMockRepository);
const postService = new PostService({ postRepository: postMockInstance } as Dependencies);
const spyService = spy(postService);

const userMockRepository = mock(Repository) as Repository<User>;
const userMockInstance = instance(userMockRepository);
const userService = new UserService({ userRepository: userMockInstance } as Dependencies);

let mockReq: Partial<Request> = {};
let mockRes: Partial<Response> = {};
const mockNext: ErrorRequestHandler = () => {};

const testUser = new User('avatar');
const testPost = new Post(testUser, 'category', 'message');

const postController = new PostController({
  userService: userService,
  postService: postService,
  validatePost: validatePost,
  validatePostPUT: validatePostPUT,
  validateParams: validateParams
} as Dependencies);

test('readAll', () => {
  when(spyService.find()).thenResolve([]);
  postController.readAll(mockReq as Request, mockRes as Response, mockNext as NextFunction);
  verify(spyService.find()).called();
});

test('readOne return with value', () => {
  when(spyService.findOne(1)).thenResolve(testPost);
  mockReq.params = { id: 1 };
  postController.readOne(mockReq as Request, mockRes as Response, mockNext as NextFunction);
  verify(spyService.findOne(1)).called();
});

test('readOne return with value', () => {
  when(spyService.findOne(99)).thenResolve(undefined);
  mockReq.params = { id: 99 };
  postController.readOne(mockReq as Request, mockRes as Response, mockNext as NextFunction);
  verify(spyService.findOne(99)).called();
});

test('create post with success', () => {
  when(spyService.save(1, 'category', 'message', userService)).thenResolve(testPost);
  mockReq.body = { ownerId: 1, category: 'category', message: 'message' };
  postController.create(mockReq as Request, mockRes as Response, mockNext as NextFunction);
  verify(spyService.save(1, 'category', 'message', userService)).called();
});

test('create post with fail in finding user', () => {
  when(spyService.save(99, 'category', 'message', userService)).thenResolve(undefined);
  mockReq.body = { ownerId: 99, category: 'category', message: 'message' };
  postController.create(mockReq as Request, mockRes as Response, mockNext as NextFunction);
  verify(spyService.save(99, 'category', 'message', userService)).called();
});

test('delete post', () => {
  const result = new DeleteResult();
  result.affected = 1;
  when(spyService.delete(1)).thenResolve(result);
  mockReq.params = { id: 1 };
  postController.delete(mockReq as Request, mockRes as Response, mockNext as NextFunction);
  verify(spyService.delete(1)).called();
});

test("delete post that doesn't exist", () => {
  const result = new DeleteResult();
  result.affected = 0;
  when(spyService.delete(99)).thenResolve(result);
  mockReq.params = { id: 99 };
  postController.delete(mockReq as Request, mockRes as Response, mockNext as NextFunction);
  verify(spyService.delete(99)).called();
});

test('update post', () => {
  when(spyService.update(1, deepEqual({ message: 'message', category: 'category' }))).thenResolve(
    testPost
  );
  mockReq.body = testPost;
  postController.update(mockReq as Request, mockRes as Response, mockNext as NextFunction);
  verify(spyService.update(1, deepEqual({ message: 'message', category: 'category' })));
});

test('update post when user is not found', () => {
  when(spyService.update(1, deepEqual({ message: 'message', category: 'category' }))).thenResolve(
    undefined
  );
  mockReq.body = testPost;
  postController.update(mockReq as Request, mockRes as Response, mockNext as NextFunction);
  verify(spyService.update(1, deepEqual({ message: 'message', category: 'category' })));
});