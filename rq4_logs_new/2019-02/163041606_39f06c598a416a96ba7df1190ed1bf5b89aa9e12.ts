export type Maybe<T> = T | null;

export interface PostWhereInput {
  id: string;
}

export interface PostsWhereInput {
  userId: string;
}

export interface UserWhereInput {
  id: string;
}

export enum CacheControlScope {
  Public = "PUBLIC",
  Private = "PRIVATE"
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
  post: Post;

  posts: Post[];

  user: User;

  users: User[];
}

export interface Post {
  id: string;

  title: string;

  body: string;
}

export interface User {
  posts?: Maybe<Post[]>;

  id: string;

  name: string;

  username: string;

  email: string;
}

// ====================================================
// Arguments
// ====================================================

export interface PostQueryArgs {
  where: PostWhereInput;
}
export interface PostsQueryArgs {
  where: PostsWhereInput;
}
export interface UserQueryArgs {
  where: UserWhereInput;
}