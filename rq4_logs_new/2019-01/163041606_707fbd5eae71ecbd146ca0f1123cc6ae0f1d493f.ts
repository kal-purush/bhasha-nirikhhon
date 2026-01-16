export type Maybe<T> = T | null;

export interface UserWhereInput {
  id: string;
}

export interface PostWhereInput {
  id: string;
}

export interface PostsWhereInput {
  userId: string;
}

export enum CacheControlScope {
  Public = 'PUBLIC',
  Private = 'PRIVATE',
}

/** The `Upload` scalar type represents a file upload. */
export type Upload = any;

// ====================================================
// Scalars
// ====================================================

// ====================================================
// Types
// ====================================================

export interface Query {
  user: User;

  users: User[];

  post: Post;

  posts: Post[];
}

export interface User {
  id: string;

  name: string;

  username: string;

  email: string;
}

export interface Post {
  id: string;

  title: string;

  body: string;
}

// ====================================================
// Arguments
// ====================================================

export interface UserQueryArgs {
  where: UserWhereInput;
}
export interface PostQueryArgs {
  where: PostWhereInput;
}
export interface PostsQueryArgs {
  where: PostsWhereInput;
}