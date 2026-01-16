export type Maybe<T> = T | null;

export interface PostWhereInput {
  id: string;
}

export interface PostsWhereInput {
  userId: string;
}

// ====================================================
// Types
// ====================================================

export interface Query {
  post: Post;

  posts: Post[];
}

export interface Post {
  id: string;

  title: string;

  body: string;
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

import { GraphQLResolveInfo } from "graphql";

export type Resolver<Result, Parent = {}, Context = {}, Args = {}> = (
  parent: Parent,
  args: Args,
  context: Context,
  info: GraphQLResolveInfo
) => Promise<Result> | Result;

export interface ISubscriptionResolverObject<Result, Parent, Context, Args> {
  subscribe<R = Result, P = Parent>(
    parent: P,
    args: Args,
    context: Context,
    info: GraphQLResolveInfo
  ): AsyncIterator<R | Result> | Promise<AsyncIterator<R | Result>>;
  resolve?<R = Result, P = Parent>(
    parent: P,
    args: Args,
    context: Context,
    info: GraphQLResolveInfo
  ): R | Result | Promise<R | Result>;
}

export type SubscriptionResolver<
  Result,
  Parent = {},
  Context = {},
  Args = {}
> =
  | ((
      ...args: any[]
    ) => ISubscriptionResolverObject<Result, Parent, Context, Args>)
  | ISubscriptionResolverObject<Result, Parent, Context, Args>;

export type TypeResolveFn<Types, Parent = {}, Context = {}> = (
  parent: Parent,
  context: Context,
  info: GraphQLResolveInfo
) => Maybe<Types>;

export type NextResolverFn<T> = () => Promise<T>;

export type DirectiveResolverFn<TResult, TArgs = {}, TContext = {}> = (
  next: NextResolverFn<TResult>,
  source: any,
  args: TArgs,
  context: TContext,
  info: GraphQLResolveInfo
) => TResult | Promise<TResult>;

export interface QueryResolvers<Context = {}, TypeParent = {}> {
  post?: QueryPostResolver<Post, TypeParent, Context>;

  posts?: QueryPostsResolver<Post[], TypeParent, Context>;
}

export type QueryPostResolver<R = Post, Parent = {}, Context = {}> = Resolver<
  R,
  Parent,
  Context,
  QueryPostArgs
>;
export interface QueryPostArgs {
  where: PostWhereInput;
}

export type QueryPostsResolver<
  R = Post[],
  Parent = {},
  Context = {}
> = Resolver<R, Parent, Context, QueryPostsArgs>;
export interface QueryPostsArgs {
  where: PostsWhereInput;
}

export interface PostResolvers<Context = {}, TypeParent = Post> {
  id?: PostIdResolver<string, TypeParent, Context>;

  title?: PostTitleResolver<string, TypeParent, Context>;

  body?: PostBodyResolver<string, TypeParent, Context>;
}

export type PostIdResolver<R = string, Parent = Post, Context = {}> = Resolver<
  R,
  Parent,
  Context
>;
export type PostTitleResolver<
  R = string,
  Parent = Post,
  Context = {}
> = Resolver<R, Parent, Context>;
export type PostBodyResolver<
  R = string,
  Parent = Post,
  Context = {}
> = Resolver<R, Parent, Context>;

/** Directs the executor to skip this field or fragment when the `if` argument is true. */
export type SkipDirectiveResolver<Result> = DirectiveResolverFn<
  Result,
  SkipDirectiveArgs,
  {}
>;
export interface SkipDirectiveArgs {
  /** Skipped when true. */
  if: boolean;
}

/** Directs the executor to include this field or fragment only when the `if` argument is true. */
export type IncludeDirectiveResolver<Result> = DirectiveResolverFn<
  Result,
  IncludeDirectiveArgs,
  {}
>;
export interface IncludeDirectiveArgs {
  /** Included when true. */
  if: boolean;
}

/** Marks an element of a GraphQL schema as no longer supported. */
export type DeprecatedDirectiveResolver<Result> = DirectiveResolverFn<
  Result,
  DeprecatedDirectiveArgs,
  {}
>;
export interface DeprecatedDirectiveArgs {
  /** Explains why this element was deprecated, usually also including a suggestion for how to access supported similar data. Formatted using the Markdown syntax (as specified by [CommonMark](https://commonmark.org/). */
  reason?: string;
}

export interface IResolvers<Context = {}> {
  Query?: QueryResolvers<Context>;
  Post?: PostResolvers<Context>;
}

export interface IDirectiveResolvers<Result> {
  skip?: SkipDirectiveResolver<Result>;
  include?: IncludeDirectiveResolver<Result>;
  deprecated?: DeprecatedDirectiveResolver<Result>;
}