import { mock, instance, when, verify, deepEqual, spy } from 'ts-mockito';
import { Dependencies } from '../types';
import { Repository, DeleteResult, UpdateResult } from 'typeorm';
import { Post } from '../entities/post';
import { PostService } from '../service/post-service';
import { User } from '../entities/user';
import { UserService } from '../service/user-service';

// Creating mock
const mockRepository = <Repository<Post>>mock(Repository);
const repoInstance = instance(mockRepository);
const service = new PostService(<Dependencies>{ postRepository: repoInstance });
const user = new User('test');
const testPost = new Post(user, 'category', 'message', 1);

const userRepository = <Repository<User>>mock(Repository);
const userRepoInstance = instance(userRepository);
const userService = new UserService({ userRepository: userRepoInstance } as Dependencies);
const userServiceSpy = spy(userService);

test('Fetch empty collection of posts', () => {
  when(mockRepository.find()).thenResolve(new Array<Post>());

  service
    .find()
    .then(collection => {
      expect(collection).toStrictEqual([]);
    })
    .catch(error => console.log(error));

  verify(mockRepository.find()).called();
});

test('Fetch a post that exists', () => {
  when(mockRepository.findOne(2)).thenResolve(testPost);
  service
    .findOne(2)
    .then(post => {
      expect(post).toStrictEqual(testPost);
    })
    .catch(error => console.log(error));
  verify(mockRepository.findOne(2)).called();
});

test("Fetch a post that doesn't exist", () => {
  when(mockRepository.findOne(99)).thenResolve(undefined);

  service
    .findOne(99)
    .then(post => {
      expect(post).toBe(undefined);
    })
    .catch(error => console.log(error));

  verify(mockRepository.findOne(99)).called();
});

test('Delete post that exists', () => {
  let results = new DeleteResult();
  results.affected = 1;
  when(mockRepository.delete(2)).thenResolve(results);

  service
    .delete(2)
    .then(deleted => {
      expect(deleted.affected).toBe(1);
    })
    .catch(error => console.log(error));

  verify(mockRepository.delete(2)).called();
});

test("Delete post that doesn't exist", () => {
  let results = new DeleteResult();
  results.affected = 0;
  when(mockRepository.delete(99)).thenResolve(results);

  service
    .delete(99)
    .then(deleted => {
      expect(deleted.affected).toBe(0);
    })
    .catch(error => console.log(error));

  verify(mockRepository.delete(99)).called();
});

test('Update post that exists', () => {
  let results = new UpdateResult();
  when(mockRepository.update(2, deepEqual(testPost))).thenResolve(results);
  when(mockRepository.findOne(2)).thenResolve(testPost);
  service
    .update(2, testPost)
    .then(result => {
      expect(result).toStrictEqual(testPost);
    })
    .catch(error => console.log(error));
  verify(mockRepository.update(2, deepEqual(testPost))).called();
  verify(mockRepository.findOne(2)).called();
});

test("Update post that doesn't exist", () => {
  let results = new UpdateResult();
  when(mockRepository.update(99, deepEqual(testPost))).thenResolve(results);
  when(mockRepository.findOne(99)).thenResolve(undefined);
  service
    .update(99, testPost)
    .then(result => {
      expect(result).toBe(undefined);
    })
    .catch(error => console.log(error));
  verify(mockRepository.update(99, deepEqual(testPost))).called();
  verify(mockRepository.findOne(99)).called();
});

test('Save a new post', () => {
  when(mockRepository.save(deepEqual(testPost))).thenResolve(testPost);
  when(userRepository.findOne(1)).thenResolve(user);
  service
    .save(1, 'category', 'message', userService)
    .then(result => {
      expect(result).toStrictEqual(testPost);
    })
    .catch(error => console.log(error));
  verify(userServiceSpy.findOne(1)).called();
});

test('Fail saving post because of no user', () => {
  when(userRepository.findOne(1)).thenResolve(undefined);
  service
    .save(1, 'category', 'message', userService)
    .then(result => {
      expect(result).toBe(undefined);
    })
    .catch(error => console.log(error));
  verify(userServiceSpy.findOne(1)).called();
});