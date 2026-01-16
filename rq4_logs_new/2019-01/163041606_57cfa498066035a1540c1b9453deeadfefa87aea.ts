export type Maybe<T> = T | null;

export enum CacheControlScope {
  Public = "PUBLIC",
  Private = "PRIVATE"
}

/** The `Upload` scalar type represents a file upload. */
export type Upload = any;

// ====================================================
// Documents
// ====================================================

export type GetPostsVariables = {
  userId: string;
};

export type GetPostsQuery = {
  __typename?: "Query";

  posts: GetPostsPosts[];
};

export type GetPostsPosts = {
  __typename?: "Post";

  id: string;

  title: string;

  body: string;
};

export type GetUsersVariables = {};

export type GetUsersQuery = {
  __typename?: "Query";

  users: GetUsersUsers[];
};

export type GetUsersUsers = {
  __typename?: "User";

  id: string;

  name: string;
};