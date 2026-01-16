import { http } from '@/api';
import { CommentsResponse } from './types';

export const postLike = (postId: number) =>
  http.post({ url: `/posts/likes/${postId}` });

export const postComment = ({
  postId,
  content,
  parentId,
}: {
  postId: number;
  content: string;
  parentId?: number;
}) =>
  http.post({
    url: `/posts/comments/${postId}`,
    data: { content, parentId: parentId || null },
  });

export const getComments = (id: number, page: number) =>
  http.get<CommentsResponse>({
    url: `/posts/comments/${id}`,
    params: { page },
  });

export const getChildComments = (id: number, page: number) =>
  http.get<CommentsResponse>({
    url: `/posts/comments/childs/${id}`,
    params: { page },
  });