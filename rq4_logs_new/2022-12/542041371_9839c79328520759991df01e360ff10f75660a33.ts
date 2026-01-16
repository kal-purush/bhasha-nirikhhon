import { ApiResponse } from '../../../types/api';
import ForumAPI from '../../../api/forum';
import {
  ForumEntityTransformed,
  ForumThreadTransformed,
  LikeDislike,
  User,
  UserId,
} from '../../../types/forum';
import {
  transformForumData,
  transformForumDTOtoStore,
} from '../../../utils/forum-utils';
import { UserDTO } from '../../../types/user';

const getForum = async (): ApiResponse<ForumThreadTransformed[]> => {
  const response = await ForumAPI.getForum();
  if (response.error || !response.data) {
    return { error: 'Не удалось получить данные с форума' };
  }
  const { threads, posts, comments, users } = response.data;
  if (!threads || !posts || !comments || !users) {
    return { error: 'Неверный формат данных форума' };
  }
  const transformedForum = transformForumData(response.data);
  return { data: transformedForum };
};

export async function forumSignIn(userRes: UserDTO): ApiResponse<UserDTO> {
  // temporary not safe solution here
  const forumAuthRes = await ForumAPI.signIn(userRes);
  if (forumAuthRes.error || !forumAuthRes.data) {
    return { error: 'Не удалось авторизоваться на форуме' };
  }
  const { userId } = forumAuthRes.data;
  return { data: { ...userRes, forumId: userId } };
}

const createThread = async (payload: {
  content: string;
  user: UserDTO;
}): ApiResponse<ForumEntityTransformed> => {
  const { user } = payload;
  const content = payload.content.trim();
  if (!user.forumId || content.length === 0) {
    return { error: 'Введите корректные данные' };
  }
  const response = await ForumAPI.createThread({
    content,
    userId: user.forumId,
  });
  if (response.error || !response.data) {
    return { error: 'Не удалось создать тему' };
  }
  const { data } = response;
  const transformedThread = transformForumDTOtoStore('thread', data, user);
  if (!transformedThread) {
    return { error: 'Не удалось создать тему' };
  }
  return { data: transformedThread };
};

const createPost = async (payload: {
  content: string;
  user: UserDTO;
  threadId: number;
}): ApiResponse<ForumEntityTransformed> => {
  const { user, threadId } = payload;
  const content = payload.content.trim();
  if (!user.forumId || content.length === 0) {
    return { error: 'Введите корректные данные' };
  }
  const response = await ForumAPI.createPost({
    content,
    threadId,
    userId: user.forumId,
  });
  if (response.error || !response.data) {
    return { error: 'Не удалось создать пост' };
  }
  const { data } = response;
  const transformedPost = {
    ...transformForumDTOtoStore('post', data, user),
    threadId,
  };
  if (!transformedPost) {
    return { error: 'Не удалось создать пост' };
  }
  return { data: transformedPost };
};

const createComment = async (payload: {
  content: string;
  user: UserDTO;
  postId: number;
}): ApiResponse<ForumEntityTransformed> => {
  const { user, postId } = payload;
  const content = payload.content.trim();
  if (!user.forumId || content.length === 0) {
    return { error: 'Введите корректные данные' };
  }
  const response = await ForumAPI.createComment({
    content,
    postId,
    userId: user.forumId,
  });
  if (response.error || !response.data) {
    return { error: 'Не удалось создать комментарий' };
  }
  const { data } = response;
  const transformedComment = {
    ...transformForumDTOtoStore('comment', data, user),
    postId,
  };
  if (!transformedComment) {
    return { error: 'Не удалось создать комментарий' };
  }
  return { data: transformedComment };
};

const setLike = async (
  payload: LikeDislike & User
): ApiResponse<LikeDislike & UserId> => {
  const { user, threadId, postId, commentId } = payload;
  if (!user.forumId) {
    return { error: 'Не удалось получить id пользователя на форуме' };
  }
  const response = await ForumAPI.setLike({
    userId: user.forumId,
    threadId,
    postId,
    commentId,
  });
  if (response.error || !response.data) {
    return { error: 'Не удалось поставить лайк' };
  }
  return { data: { ...response.data, threadId } };
};

const setDislike = async (
  payload: LikeDislike & User
): ApiResponse<LikeDislike & UserId> => {
  const { user, threadId, postId, commentId } = payload;
  if (!user.forumId) {
    return { error: 'Не удалось получить id пользователя на форуме' };
  }
  const response = await ForumAPI.setDislike({
    userId: user.forumId,
    threadId,
    postId,
    commentId,
  });
  if (response.error || !response.data) {
    return { error: 'Не удалось поставить дислайк' };
  }
  return { data: { ...response.data, threadId } };
};

const forumService = {
  getForum,
  createThread,
  createPost,
  createComment,
  setLike,
  setDislike,
};

export default forumService;