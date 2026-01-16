export interface ProjectWhereInput {
  /** Logical AND on all given filters. */
  AND?: ProjectWhereInput[] | null;
  /** Logical OR on all given filters. */
  OR?: ProjectWhereInput[] | null;
  /** Logical NOT on all given filters combined by AND. */
  NOT?: ProjectWhereInput[] | null;

  id?: string | null;
  /** All values that are not equal to given value. */
  id_not?: string | null;
  /** All values that are contained in given list. */
  id_in?: string[] | null;
  /** All values that are not contained in given list. */
  id_not_in?: string[] | null;
  /** All values less than the given value. */
  id_lt?: string | null;
  /** All values less than or equal the given value. */
  id_lte?: string | null;
  /** All values greater than the given value. */
  id_gt?: string | null;
  /** All values greater than or equal the given value. */
  id_gte?: string | null;
  /** All values containing the given string. */
  id_contains?: string | null;
  /** All values not containing the given string. */
  id_not_contains?: string | null;
  /** All values starting with the given string. */
  id_starts_with?: string | null;
  /** All values not starting with the given string. */
  id_not_starts_with?: string | null;
  /** All values ending with the given string. */
  id_ends_with?: string | null;
  /** All values not ending with the given string. */
  id_not_ends_with?: string | null;

  name?: string | null;
  /** All values that are not equal to given value. */
  name_not?: string | null;
  /** All values that are contained in given list. */
  name_in?: string[] | null;
  /** All values that are not contained in given list. */
  name_not_in?: string[] | null;
  /** All values less than the given value. */
  name_lt?: string | null;
  /** All values less than or equal the given value. */
  name_lte?: string | null;
  /** All values greater than the given value. */
  name_gt?: string | null;
  /** All values greater than or equal the given value. */
  name_gte?: string | null;
  /** All values containing the given string. */
  name_contains?: string | null;
  /** All values not containing the given string. */
  name_not_contains?: string | null;
  /** All values starting with the given string. */
  name_starts_with?: string | null;
  /** All values not starting with the given string. */
  name_not_starts_with?: string | null;
  /** All values ending with the given string. */
  name_ends_with?: string | null;
  /** All values not ending with the given string. */
  name_not_ends_with?: string | null;

  description?: string | null;
  /** All values that are not equal to given value. */
  description_not?: string | null;
  /** All values that are contained in given list. */
  description_in?: string[] | null;
  /** All values that are not contained in given list. */
  description_not_in?: string[] | null;
  /** All values less than the given value. */
  description_lt?: string | null;
  /** All values less than or equal the given value. */
  description_lte?: string | null;
  /** All values greater than the given value. */
  description_gt?: string | null;
  /** All values greater than or equal the given value. */
  description_gte?: string | null;
  /** All values containing the given string. */
  description_contains?: string | null;
  /** All values not containing the given string. */
  description_not_contains?: string | null;
  /** All values starting with the given string. */
  description_starts_with?: string | null;
  /** All values not starting with the given string. */
  description_not_starts_with?: string | null;
  /** All values ending with the given string. */
  description_ends_with?: string | null;
  /** All values not ending with the given string. */
  description_not_ends_with?: string | null;
}

export interface UserWhereInput {
  /** Logical AND on all given filters. */
  AND?: UserWhereInput[] | null;
  /** Logical OR on all given filters. */
  OR?: UserWhereInput[] | null;
  /** Logical NOT on all given filters combined by AND. */
  NOT?: UserWhereInput[] | null;

  id?: string | null;
  /** All values that are not equal to given value. */
  id_not?: string | null;
  /** All values that are contained in given list. */
  id_in?: string[] | null;
  /** All values that are not contained in given list. */
  id_not_in?: string[] | null;
  /** All values less than the given value. */
  id_lt?: string | null;
  /** All values less than or equal the given value. */
  id_lte?: string | null;
  /** All values greater than the given value. */
  id_gt?: string | null;
  /** All values greater than or equal the given value. */
  id_gte?: string | null;
  /** All values containing the given string. */
  id_contains?: string | null;
  /** All values not containing the given string. */
  id_not_contains?: string | null;
  /** All values starting with the given string. */
  id_starts_with?: string | null;
  /** All values not starting with the given string. */
  id_not_starts_with?: string | null;
  /** All values ending with the given string. */
  id_ends_with?: string | null;
  /** All values not ending with the given string. */
  id_not_ends_with?: string | null;

  email?: string | null;
  /** All values that are not equal to given value. */
  email_not?: string | null;
  /** All values that are contained in given list. */
  email_in?: string[] | null;
  /** All values that are not contained in given list. */
  email_not_in?: string[] | null;
  /** All values less than the given value. */
  email_lt?: string | null;
  /** All values less than or equal the given value. */
  email_lte?: string | null;
  /** All values greater than the given value. */
  email_gt?: string | null;
  /** All values greater than or equal the given value. */
  email_gte?: string | null;
  /** All values containing the given string. */
  email_contains?: string | null;
  /** All values not containing the given string. */
  email_not_contains?: string | null;
  /** All values starting with the given string. */
  email_starts_with?: string | null;
  /** All values not starting with the given string. */
  email_not_starts_with?: string | null;
  /** All values ending with the given string. */
  email_ends_with?: string | null;
  /** All values not ending with the given string. */
  email_not_ends_with?: string | null;
}

export interface ProjectWhereUniqueInput {
  id?: string | null;
}

export interface UserWhereUniqueInput {
  id?: string | null;

  email?: string | null;
}

export interface ProjectCreateInput {
  name: string;

  description: string;
}

export interface UserCreateInput {
  email: string;
}

export interface ProjectUpdateInput {
  name?: string | null;

  description?: string | null;
}

export interface UserUpdateInput {
  email?: string | null;
}

export interface ProjectUpdateManyMutationInput {
  name?: string | null;

  description?: string | null;
}

export interface UserUpdateManyMutationInput {
  email?: string | null;
}

export interface ProjectSubscriptionWhereInput {
  /** Logical AND on all given filters. */
  AND?: ProjectSubscriptionWhereInput[] | null;
  /** Logical OR on all given filters. */
  OR?: ProjectSubscriptionWhereInput[] | null;
  /** Logical NOT on all given filters combined by AND. */
  NOT?: ProjectSubscriptionWhereInput[] | null;
  /** The subscription event gets dispatched when it's listed in mutation_in */
  mutation_in?: MutationType[] | null;
  /** The subscription event gets only dispatched when one of the updated fields names is included in this list */
  updatedFields_contains?: string | null;
  /** The subscription event gets only dispatched when all of the field names included in this list have been updated */
  updatedFields_contains_every?: string[] | null;
  /** The subscription event gets only dispatched when some of the field names included in this list have been updated */
  updatedFields_contains_some?: string[] | null;

  node?: ProjectWhereInput | null;
}

export interface UserSubscriptionWhereInput {
  /** Logical AND on all given filters. */
  AND?: UserSubscriptionWhereInput[] | null;
  /** Logical OR on all given filters. */
  OR?: UserSubscriptionWhereInput[] | null;
  /** Logical NOT on all given filters combined by AND. */
  NOT?: UserSubscriptionWhereInput[] | null;
  /** The subscription event gets dispatched when it's listed in mutation_in */
  mutation_in?: MutationType[] | null;
  /** The subscription event gets only dispatched when one of the updated fields names is included in this list */
  updatedFields_contains?: string | null;
  /** The subscription event gets only dispatched when all of the field names included in this list have been updated */
  updatedFields_contains_every?: string[] | null;
  /** The subscription event gets only dispatched when some of the field names included in this list have been updated */
  updatedFields_contains_some?: string[] | null;

  node?: UserWhereInput | null;
}

export enum ProjectOrderByInput {
  IdAsc = "id_ASC",
  IdDesc = "id_DESC",
  NameAsc = "name_ASC",
  NameDesc = "name_DESC",
  DescriptionAsc = "description_ASC",
  DescriptionDesc = "description_DESC",
  UpdatedAtAsc = "updatedAt_ASC",
  UpdatedAtDesc = "updatedAt_DESC",
  CreatedAtAsc = "createdAt_ASC",
  CreatedAtDesc = "createdAt_DESC"
}

export enum UserOrderByInput {
  IdAsc = "id_ASC",
  IdDesc = "id_DESC",
  EmailAsc = "email_ASC",
  EmailDesc = "email_DESC",
  UpdatedAtAsc = "updatedAt_ASC",
  UpdatedAtDesc = "updatedAt_DESC",
  CreatedAtAsc = "createdAt_ASC",
  CreatedAtDesc = "createdAt_DESC"
}

export enum MutationType {
  Created = "CREATED",
  Updated = "UPDATED",
  Deleted = "DELETED"
}

/** The `Long` scalar type represents non-fractional signed whole numeric values.Long can represent values between -(2^63) and 2^63 - 1. */
export type Long = any;
import { GraphQLResolveInfo, GraphQLScalarTypeConfig } from "graphql";

import { MyContext } from "./types/Context";

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

type Maybe<T> = T | null | undefined;

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

export namespace QueryResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = {}> {
    projects?: ProjectsResolver<(Project | null)[], TypeParent, Context>;

    users?: UsersResolver<(User | null)[], TypeParent, Context>;

    project?: ProjectResolver<Project | null, TypeParent, Context>;

    user?: UserResolver<User | null, TypeParent, Context>;

    projectsConnection?: ProjectsConnectionResolver<
      ProjectConnection,
      TypeParent,
      Context
    >;

    usersConnection?: UsersConnectionResolver<
      UserConnection,
      TypeParent,
      Context
    >;
    /** Fetches an object given its ID */
    node?: NodeResolver<Node | null, TypeParent, Context>;
  }

  export type ProjectsResolver<
    R = (Project | null)[],
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, ProjectsArgs>;
  export interface ProjectsArgs {
    where?: ProjectWhereInput | null;

    orderBy?: ProjectOrderByInput | null;

    skip?: number | null;

    after?: string | null;

    before?: string | null;

    first?: number | null;

    last?: number | null;
  }

  export type UsersResolver<
    R = (User | null)[],
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, UsersArgs>;
  export interface UsersArgs {
    where?: UserWhereInput | null;

    orderBy?: UserOrderByInput | null;

    skip?: number | null;

    after?: string | null;

    before?: string | null;

    first?: number | null;

    last?: number | null;
  }

  export type ProjectResolver<
    R = Project | null,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, ProjectArgs>;
  export interface ProjectArgs {
    where: ProjectWhereUniqueInput;
  }

  export type UserResolver<
    R = User | null,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, UserArgs>;
  export interface UserArgs {
    where: UserWhereUniqueInput;
  }

  export type ProjectsConnectionResolver<
    R = ProjectConnection,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, ProjectsConnectionArgs>;
  export interface ProjectsConnectionArgs {
    where?: ProjectWhereInput | null;

    orderBy?: ProjectOrderByInput | null;

    skip?: number | null;

    after?: string | null;

    before?: string | null;

    first?: number | null;

    last?: number | null;
  }

  export type UsersConnectionResolver<
    R = UserConnection,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, UsersConnectionArgs>;
  export interface UsersConnectionArgs {
    where?: UserWhereInput | null;

    orderBy?: UserOrderByInput | null;

    skip?: number | null;

    after?: string | null;

    before?: string | null;

    first?: number | null;

    last?: number | null;
  }

  export type NodeResolver<
    R = Node | null,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, NodeArgs>;
  export interface NodeArgs {
    /** The ID of an object */
    id: string;
  }
}

export namespace ProjectResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = Project> {
    id?: IdResolver<string, TypeParent, Context>;

    name?: NameResolver<string, TypeParent, Context>;

    description?: DescriptionResolver<string, TypeParent, Context>;
  }

  export type IdResolver<
    R = string,
    Parent = Project,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type NameResolver<
    R = string,
    Parent = Project,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type DescriptionResolver<
    R = string,
    Parent = Project,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}

export namespace UserResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = User> {
    id?: IdResolver<string, TypeParent, Context>;

    email?: EmailResolver<string, TypeParent, Context>;
  }

  export type IdResolver<
    R = string,
    Parent = User,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type EmailResolver<
    R = string,
    Parent = User,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}
/** A connection to a list of items. */
export namespace ProjectConnectionResolvers {
  export interface Resolvers<
    Context = MyContext,
    TypeParent = ProjectConnection
  > {
    /** Information to aid in pagination. */
    pageInfo?: PageInfoResolver<PageInfo, TypeParent, Context>;
    /** A list of edges. */
    edges?: EdgesResolver<(ProjectEdge | null)[], TypeParent, Context>;

    aggregate?: AggregateResolver<AggregateProject, TypeParent, Context>;
  }

  export type PageInfoResolver<
    R = PageInfo,
    Parent = ProjectConnection,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type EdgesResolver<
    R = (ProjectEdge | null)[],
    Parent = ProjectConnection,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type AggregateResolver<
    R = AggregateProject,
    Parent = ProjectConnection,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}
/** Information about pagination in a connection. */
export namespace PageInfoResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = PageInfo> {
    /** When paginating forwards, are there more items? */
    hasNextPage?: HasNextPageResolver<boolean, TypeParent, Context>;
    /** When paginating backwards, are there more items? */
    hasPreviousPage?: HasPreviousPageResolver<boolean, TypeParent, Context>;
    /** When paginating backwards, the cursor to continue. */
    startCursor?: StartCursorResolver<string | null, TypeParent, Context>;
    /** When paginating forwards, the cursor to continue. */
    endCursor?: EndCursorResolver<string | null, TypeParent, Context>;
  }

  export type HasNextPageResolver<
    R = boolean,
    Parent = PageInfo,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type HasPreviousPageResolver<
    R = boolean,
    Parent = PageInfo,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type StartCursorResolver<
    R = string | null,
    Parent = PageInfo,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type EndCursorResolver<
    R = string | null,
    Parent = PageInfo,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}
/** An edge in a connection. */
export namespace ProjectEdgeResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = ProjectEdge> {
    /** The item at the end of the edge. */
    node?: NodeResolver<Project, TypeParent, Context>;
    /** A cursor for use in pagination. */
    cursor?: CursorResolver<string, TypeParent, Context>;
  }

  export type NodeResolver<
    R = Project,
    Parent = ProjectEdge,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type CursorResolver<
    R = string,
    Parent = ProjectEdge,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}

export namespace AggregateProjectResolvers {
  export interface Resolvers<
    Context = MyContext,
    TypeParent = AggregateProject
  > {
    count?: CountResolver<number, TypeParent, Context>;
  }

  export type CountResolver<
    R = number,
    Parent = AggregateProject,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}
/** A connection to a list of items. */
export namespace UserConnectionResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = UserConnection> {
    /** Information to aid in pagination. */
    pageInfo?: PageInfoResolver<PageInfo, TypeParent, Context>;
    /** A list of edges. */
    edges?: EdgesResolver<(UserEdge | null)[], TypeParent, Context>;

    aggregate?: AggregateResolver<AggregateUser, TypeParent, Context>;
  }

  export type PageInfoResolver<
    R = PageInfo,
    Parent = UserConnection,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type EdgesResolver<
    R = (UserEdge | null)[],
    Parent = UserConnection,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type AggregateResolver<
    R = AggregateUser,
    Parent = UserConnection,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}
/** An edge in a connection. */
export namespace UserEdgeResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = UserEdge> {
    /** The item at the end of the edge. */
    node?: NodeResolver<User, TypeParent, Context>;
    /** A cursor for use in pagination. */
    cursor?: CursorResolver<string, TypeParent, Context>;
  }

  export type NodeResolver<
    R = User,
    Parent = UserEdge,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type CursorResolver<
    R = string,
    Parent = UserEdge,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}

export namespace AggregateUserResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = AggregateUser> {
    count?: CountResolver<number, TypeParent, Context>;
  }

  export type CountResolver<
    R = number,
    Parent = AggregateUser,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}

export namespace MutationResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = {}> {
    createProject?: CreateProjectResolver<Project, TypeParent, Context>;

    createUser?: CreateUserResolver<User, TypeParent, Context>;

    updateProject?: UpdateProjectResolver<Project | null, TypeParent, Context>;

    updateUser?: UpdateUserResolver<User | null, TypeParent, Context>;

    deleteProject?: DeleteProjectResolver<Project | null, TypeParent, Context>;

    deleteUser?: DeleteUserResolver<User | null, TypeParent, Context>;

    upsertProject?: UpsertProjectResolver<Project, TypeParent, Context>;

    upsertUser?: UpsertUserResolver<User, TypeParent, Context>;

    updateManyProjects?: UpdateManyProjectsResolver<
      BatchPayload,
      TypeParent,
      Context
    >;

    updateManyUsers?: UpdateManyUsersResolver<
      BatchPayload,
      TypeParent,
      Context
    >;

    deleteManyProjects?: DeleteManyProjectsResolver<
      BatchPayload,
      TypeParent,
      Context
    >;

    deleteManyUsers?: DeleteManyUsersResolver<
      BatchPayload,
      TypeParent,
      Context
    >;
  }

  export type CreateProjectResolver<
    R = Project,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, CreateProjectArgs>;
  export interface CreateProjectArgs {
    data: ProjectCreateInput;
  }

  export type CreateUserResolver<
    R = User,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, CreateUserArgs>;
  export interface CreateUserArgs {
    data: UserCreateInput;
  }

  export type UpdateProjectResolver<
    R = Project | null,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, UpdateProjectArgs>;
  export interface UpdateProjectArgs {
    data: ProjectUpdateInput;

    where: ProjectWhereUniqueInput;
  }

  export type UpdateUserResolver<
    R = User | null,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, UpdateUserArgs>;
  export interface UpdateUserArgs {
    data: UserUpdateInput;

    where: UserWhereUniqueInput;
  }

  export type DeleteProjectResolver<
    R = Project | null,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, DeleteProjectArgs>;
  export interface DeleteProjectArgs {
    where: ProjectWhereUniqueInput;
  }

  export type DeleteUserResolver<
    R = User | null,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, DeleteUserArgs>;
  export interface DeleteUserArgs {
    where: UserWhereUniqueInput;
  }

  export type UpsertProjectResolver<
    R = Project,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, UpsertProjectArgs>;
  export interface UpsertProjectArgs {
    where: ProjectWhereUniqueInput;

    create: ProjectCreateInput;

    update: ProjectUpdateInput;
  }

  export type UpsertUserResolver<
    R = User,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, UpsertUserArgs>;
  export interface UpsertUserArgs {
    where: UserWhereUniqueInput;

    create: UserCreateInput;

    update: UserUpdateInput;
  }

  export type UpdateManyProjectsResolver<
    R = BatchPayload,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, UpdateManyProjectsArgs>;
  export interface UpdateManyProjectsArgs {
    data: ProjectUpdateManyMutationInput;

    where?: ProjectWhereInput | null;
  }

  export type UpdateManyUsersResolver<
    R = BatchPayload,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, UpdateManyUsersArgs>;
  export interface UpdateManyUsersArgs {
    data: UserUpdateManyMutationInput;

    where?: UserWhereInput | null;
  }

  export type DeleteManyProjectsResolver<
    R = BatchPayload,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, DeleteManyProjectsArgs>;
  export interface DeleteManyProjectsArgs {
    where?: ProjectWhereInput | null;
  }

  export type DeleteManyUsersResolver<
    R = BatchPayload,
    Parent = {},
    Context = MyContext
  > = Resolver<R, Parent, Context, DeleteManyUsersArgs>;
  export interface DeleteManyUsersArgs {
    where?: UserWhereInput | null;
  }
}

export namespace BatchPayloadResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = BatchPayload> {
    /** The number of nodes that have been affected by the Batch operation. */
    count?: CountResolver<Long, TypeParent, Context>;
  }

  export type CountResolver<
    R = Long,
    Parent = BatchPayload,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}

export namespace SubscriptionResolvers {
  export interface Resolvers<Context = MyContext, TypeParent = {}> {
    project?: ProjectResolver<
      ProjectSubscriptionPayload | null,
      TypeParent,
      Context
    >;

    user?: UserResolver<UserSubscriptionPayload | null, TypeParent, Context>;
  }

  export type ProjectResolver<
    R = ProjectSubscriptionPayload | null,
    Parent = {},
    Context = MyContext
  > = SubscriptionResolver<R, Parent, Context, ProjectArgs>;
  export interface ProjectArgs {
    where?: ProjectSubscriptionWhereInput | null;
  }

  export type UserResolver<
    R = UserSubscriptionPayload | null,
    Parent = {},
    Context = MyContext
  > = SubscriptionResolver<R, Parent, Context, UserArgs>;
  export interface UserArgs {
    where?: UserSubscriptionWhereInput | null;
  }
}

export namespace ProjectSubscriptionPayloadResolvers {
  export interface Resolvers<
    Context = MyContext,
    TypeParent = ProjectSubscriptionPayload
  > {
    mutation?: MutationResolver<MutationType, TypeParent, Context>;

    node?: NodeResolver<Project | null, TypeParent, Context>;

    updatedFields?: UpdatedFieldsResolver<string[] | null, TypeParent, Context>;

    previousValues?: PreviousValuesResolver<
      ProjectPreviousValues | null,
      TypeParent,
      Context
    >;
  }

  export type MutationResolver<
    R = MutationType,
    Parent = ProjectSubscriptionPayload,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type NodeResolver<
    R = Project | null,
    Parent = ProjectSubscriptionPayload,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type UpdatedFieldsResolver<
    R = string[] | null,
    Parent = ProjectSubscriptionPayload,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type PreviousValuesResolver<
    R = ProjectPreviousValues | null,
    Parent = ProjectSubscriptionPayload,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}

export namespace ProjectPreviousValuesResolvers {
  export interface Resolvers<
    Context = MyContext,
    TypeParent = ProjectPreviousValues
  > {
    id?: IdResolver<string, TypeParent, Context>;

    name?: NameResolver<string, TypeParent, Context>;

    description?: DescriptionResolver<string, TypeParent, Context>;
  }

  export type IdResolver<
    R = string,
    Parent = ProjectPreviousValues,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type NameResolver<
    R = string,
    Parent = ProjectPreviousValues,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type DescriptionResolver<
    R = string,
    Parent = ProjectPreviousValues,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}

export namespace UserSubscriptionPayloadResolvers {
  export interface Resolvers<
    Context = MyContext,
    TypeParent = UserSubscriptionPayload
  > {
    mutation?: MutationResolver<MutationType, TypeParent, Context>;

    node?: NodeResolver<User | null, TypeParent, Context>;

    updatedFields?: UpdatedFieldsResolver<string[] | null, TypeParent, Context>;

    previousValues?: PreviousValuesResolver<
      UserPreviousValues | null,
      TypeParent,
      Context
    >;
  }

  export type MutationResolver<
    R = MutationType,
    Parent = UserSubscriptionPayload,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type NodeResolver<
    R = User | null,
    Parent = UserSubscriptionPayload,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type UpdatedFieldsResolver<
    R = string[] | null,
    Parent = UserSubscriptionPayload,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type PreviousValuesResolver<
    R = UserPreviousValues | null,
    Parent = UserSubscriptionPayload,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}

export namespace UserPreviousValuesResolvers {
  export interface Resolvers<
    Context = MyContext,
    TypeParent = UserPreviousValues
  > {
    id?: IdResolver<string, TypeParent, Context>;

    email?: EmailResolver<string, TypeParent, Context>;
  }

  export type IdResolver<
    R = string,
    Parent = UserPreviousValues,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
  export type EmailResolver<
    R = string,
    Parent = UserPreviousValues,
    Context = MyContext
  > = Resolver<R, Parent, Context>;
}

/** An object with an ID */
export namespace NodeResolvers {
  export interface Resolvers {
    __resolveType: ResolveType;
  }
  export type ResolveType<
    R = "Project" | "User",
    Parent = Project | User,
    Context = MyContext
  > = TypeResolveFn<R, Parent, Context>;
}

/** Directs the executor to skip this field or fragment when the `if` argument is true. */
export type SkipDirectiveResolver<Result> = DirectiveResolverFn<
  Result,
  SkipDirectiveArgs,
  MyContext
>;
export interface SkipDirectiveArgs {
  /** Skipped when true. */
  if: boolean;
}

/** Directs the executor to include this field or fragment only when the `if` argument is true. */
export type IncludeDirectiveResolver<Result> = DirectiveResolverFn<
  Result,
  IncludeDirectiveArgs,
  MyContext
>;
export interface IncludeDirectiveArgs {
  /** Included when true. */
  if: boolean;
}

/** Marks an element of a GraphQL schema as no longer supported. */
export type DeprecatedDirectiveResolver<Result> = DirectiveResolverFn<
  Result,
  DeprecatedDirectiveArgs,
  MyContext
>;
export interface DeprecatedDirectiveArgs {
  /** Explains why this element was deprecated, usually also including a suggestion for how to access supported similar data. Formatted in [Markdown](https://daringfireball.net/projects/markdown/). */
  reason?: string | null;
}

export interface LongScalarConfig extends GraphQLScalarTypeConfig<Long, any> {
  name: "Long";
}

// ====================================================
// Scalars
// ====================================================

// ====================================================
// Interfaces
// ====================================================

/** An object with an ID */
export interface Node {
  /** The id of the object. */
  id: string;
}

// ====================================================
// Types
// ====================================================

export interface Query {
  projects: (Project | null)[];

  users: (User | null)[];

  project?: Project | null;

  user?: User | null;

  projectsConnection: ProjectConnection;

  usersConnection: UserConnection;
  /** Fetches an object given its ID */
  node?: Node | null;
}

export interface Project extends Node {
  id: string;

  name: string;

  description: string;
}

export interface User extends Node {
  id: string;

  email: string;
}

/** A connection to a list of items. */
export interface ProjectConnection {
  /** Information to aid in pagination. */
  pageInfo: PageInfo;
  /** A list of edges. */
  edges: (ProjectEdge | null)[];

  aggregate: AggregateProject;
}

/** Information about pagination in a connection. */
export interface PageInfo {
  /** When paginating forwards, are there more items? */
  hasNextPage: boolean;
  /** When paginating backwards, are there more items? */
  hasPreviousPage: boolean;
  /** When paginating backwards, the cursor to continue. */
  startCursor?: string | null;
  /** When paginating forwards, the cursor to continue. */
  endCursor?: string | null;
}

/** An edge in a connection. */
export interface ProjectEdge {
  /** The item at the end of the edge. */
  node: Project;
  /** A cursor for use in pagination. */
  cursor: string;
}

export interface AggregateProject {
  count: number;
}

/** A connection to a list of items. */
export interface UserConnection {
  /** Information to aid in pagination. */
  pageInfo: PageInfo;
  /** A list of edges. */
  edges: (UserEdge | null)[];

  aggregate: AggregateUser;
}

/** An edge in a connection. */
export interface UserEdge {
  /** The item at the end of the edge. */
  node: User;
  /** A cursor for use in pagination. */
  cursor: string;
}

export interface AggregateUser {
  count: number;
}

export interface Mutation {
  createProject: Project;

  createUser: User;

  updateProject?: Project | null;

  updateUser?: User | null;

  deleteProject?: Project | null;

  deleteUser?: User | null;

  upsertProject: Project;

  upsertUser: User;

  updateManyProjects: BatchPayload;

  updateManyUsers: BatchPayload;

  deleteManyProjects: BatchPayload;

  deleteManyUsers: BatchPayload;
}

export interface BatchPayload {
  /** The number of nodes that have been affected by the Batch operation. */
  count: Long;
}

export interface Subscription {
  project?: ProjectSubscriptionPayload | null;

  user?: UserSubscriptionPayload | null;
}

export interface ProjectSubscriptionPayload {
  mutation: MutationType;

  node?: Project | null;

  updatedFields?: string[] | null;

  previousValues?: ProjectPreviousValues | null;
}

export interface ProjectPreviousValues {
  id: string;

  name: string;

  description: string;
}

export interface UserSubscriptionPayload {
  mutation: MutationType;

  node?: User | null;

  updatedFields?: string[] | null;

  previousValues?: UserPreviousValues | null;
}

export interface UserPreviousValues {
  id: string;

  email: string;
}

// ====================================================
// Arguments
// ====================================================

export interface ProjectsQueryArgs {
  where?: ProjectWhereInput | null;

  orderBy?: ProjectOrderByInput | null;

  skip?: number | null;

  after?: string | null;

  before?: string | null;

  first?: number | null;

  last?: number | null;
}
export interface UsersQueryArgs {
  where?: UserWhereInput | null;

  orderBy?: UserOrderByInput | null;

  skip?: number | null;

  after?: string | null;

  before?: string | null;

  first?: number | null;

  last?: number | null;
}
export interface ProjectQueryArgs {
  where: ProjectWhereUniqueInput;
}
export interface UserQueryArgs {
  where: UserWhereUniqueInput;
}
export interface ProjectsConnectionQueryArgs {
  where?: ProjectWhereInput | null;

  orderBy?: ProjectOrderByInput | null;

  skip?: number | null;

  after?: string | null;

  before?: string | null;

  first?: number | null;

  last?: number | null;
}
export interface UsersConnectionQueryArgs {
  where?: UserWhereInput | null;

  orderBy?: UserOrderByInput | null;

  skip?: number | null;

  after?: string | null;

  before?: string | null;

  first?: number | null;

  last?: number | null;
}
export interface NodeQueryArgs {
  /** The ID of an object */
  id: string;
}
export interface CreateProjectMutationArgs {
  data: ProjectCreateInput;
}
export interface CreateUserMutationArgs {
  data: UserCreateInput;
}
export interface UpdateProjectMutationArgs {
  data: ProjectUpdateInput;

  where: ProjectWhereUniqueInput;
}
export interface UpdateUserMutationArgs {
  data: UserUpdateInput;

  where: UserWhereUniqueInput;
}
export interface DeleteProjectMutationArgs {
  where: ProjectWhereUniqueInput;
}
export interface DeleteUserMutationArgs {
  where: UserWhereUniqueInput;
}
export interface UpsertProjectMutationArgs {
  where: ProjectWhereUniqueInput;

  create: ProjectCreateInput;

  update: ProjectUpdateInput;
}
export interface UpsertUserMutationArgs {
  where: UserWhereUniqueInput;

  create: UserCreateInput;

  update: UserUpdateInput;
}
export interface UpdateManyProjectsMutationArgs {
  data: ProjectUpdateManyMutationInput;

  where?: ProjectWhereInput | null;
}
export interface UpdateManyUsersMutationArgs {
  data: UserUpdateManyMutationInput;

  where?: UserWhereInput | null;
}
export interface DeleteManyProjectsMutationArgs {
  where?: ProjectWhereInput | null;
}
export interface DeleteManyUsersMutationArgs {
  where?: UserWhereInput | null;
}
export interface ProjectSubscriptionArgs {
  where?: ProjectSubscriptionWhereInput | null;
}
export interface UserSubscriptionArgs {
  where?: UserSubscriptionWhereInput | null;
}