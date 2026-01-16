import { Indexable } from './common';
import { UserDTO } from './user';

export type ThreadDTO = Indexable<{
  threadId: number;
  content: string;
  userId: number;
  createdAt: string;
  updatedAt: string;
}>;

export type ThreadExtended = ThreadDTO & {
  posts: PostDTO[];
  user: ForumUserDTO;
  likes: ThreadLikeDTO[];
  dislikes: ThreadDislikeDTO[];
};

export type PostDTO = Indexable<{
  postId: number;
  content: string;
  threadId: number;
  userId: number;
  createdAt: string;
  updatedAt: string;
}>;

export type PostWithComments = PostDTO & {
  comments: ForumCommentExtended[];
  likes: PostLikeDTO[];
  dislikes: PostDislikeDTO[];
};

export type PostExtended = PostDTO & {
  thread: ThreadDTO;
  user: ForumUserDTO;
  comments: ForumCommentDTO[];
  likes: PostLikeDTO[];
  dislikes: PostDislikeDTO[];
};

export type ForumCommentDTO = Indexable<{
  commentId: number;
  content: string;
  postId: number;
  userId: number;
  createdAt: string;
  updatedAt: string;
}>;

export type ForumCommentExtended = ForumCommentDTO & {
  post: PostDTO;
  user: ForumUserDTO;
  likes: CommentLikeDTO[];
  dislikes: CommentDislikeDTO[];
};

export type ForumUserDTO = {
  userId: number;
  yandexId: number;
  email: string;
  name: string;
  lastname: string;
  login: string;
  phone: string;
  avatar: string;
  createdAt: string;
  updatedAt: string;
};

export type ForumUserExtended = ForumUserDTO & {
  threads: ThreadDTO[];
  posts: PostDTO[];
  comments: ForumCommentDTO[];
  threadLikes: ThreadLikeDTO[];
  threadDislikes: ThreadDislikeDTO[];
  postLikes: PostLikeDTO[];
  postDislikes: PostDislikeDTO[];
  commentLikes: CommentLikeDTO[];
  commentDislikes: CommentDislikeDTO[];
};

export type UserId = {
  userId: number;
};

export type ThreadLikeDTO = {
  threadId: number;
} & UserId;

export type ThreadDislikeDTO = {
  threadId: number;
} & UserId;

export type PostLikeDTO = {
  postId: number;
} & UserId;

export type PostDislikeDTO = {
  postId: number;
} & UserId;

export type CommentLikeDTO = {
  commentId: number;
} & UserId;

export type CommentDislikeDTO = {
  commentId: number;
} & UserId;

export type Forum = {
  threads: ThreadExtended[];
  posts: PostExtended[];
  comments: ForumCommentExtended[];
  users: ForumUserExtended[];
};

export type ForumCommonFields = {
  content: string;
  author: string;
  avatar: string;
  datetime: string;
  likes: number[];
  dislikes: number[];
};

export type ForumThreadTransformed = {
  threadId: number;
  comments: ForumPostTransformed[];
} & ForumCommonFields;

export type ForumPostTransformed = {
  postId: number;
  threadId: number;
  comments: ForumCommentTransformed[];
} & ForumCommonFields;

export type ForumCommentTransformed = {
  commentId: number;
  postId: number;
} & ForumCommonFields;

export type LikeDislike = {
  threadId?: number;
  postId?: number;
  commentId?: number;
};

export type User = {
  user: UserDTO;
};

export type CreateEntity = {
  content: string;
  threadId?: number;
  postId?: number;
  commentId?: number;
} & UserId;

export type ForumEntityTransformed =
  | ForumThreadTransformed
  | ForumPostTransformed
  | ForumCommentTransformed;