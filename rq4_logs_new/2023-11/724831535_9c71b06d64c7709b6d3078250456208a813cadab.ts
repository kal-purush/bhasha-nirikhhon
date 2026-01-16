import { GraphQLResolveInfo } from 'graphql';
import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type Maybe<T> = T | null;
export type InputMaybe<T> = Maybe<T>;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
export type MakeEmpty<T extends { [key: string]: unknown }, K extends keyof T> = { [_ in K]?: never };
export type Incremental<T> = T | { [P in keyof T]?: P extends ' $fragmentName' | '__typename' ? T[P] : never };
export type RequireFields<T, K extends keyof T> = Omit<T, K> & { [P in K]-?: NonNullable<T[P]> };
const defaultOptions = {} as const;
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: { input: string; output: string; }
  String: { input: string; output: string; }
  Boolean: { input: boolean; output: boolean; }
  Int: { input: number; output: number; }
  Float: { input: number; output: number; }
};

export type Candidate = {
  __typename?: 'Candidate';
  createdAt?: Maybe<Scalars['String']['output']>;
  email: Scalars['String']['output'];
  id: Scalars['ID']['output'];
  name?: Maybe<Scalars['String']['output']>;
  specialization?: Maybe<Scalars['String']['output']>;
  status: Scalars['String']['output'];
  updatedAt?: Maybe<Scalars['String']['output']>;
};

export type CandidateInput = {
  email: Scalars['String']['input'];
  name?: InputMaybe<Scalars['String']['input']>;
  specialization?: InputMaybe<Scalars['String']['input']>;
  status: Scalars['String']['input'];
};

export type Job = {
  __typename?: 'Job';
  createdAt?: Maybe<Scalars['String']['output']>;
  description: Scalars['String']['output'];
  id: Scalars['ID']['output'];
  openPositions: Scalars['Int']['output'];
  title: Scalars['String']['output'];
  updatedAt?: Maybe<Scalars['String']['output']>;
};

export type JobInput = {
  description: Scalars['String']['input'];
  openPositions: Scalars['Int']['input'];
  title: Scalars['String']['input'];
};

export type Mutation = {
  __typename?: 'Mutation';
  createCandidate?: Maybe<Candidate>;
  createJob?: Maybe<Job>;
  createOffer?: Maybe<OfferRaw>;
  createUser?: Maybe<User>;
};


export type MutationCreateCandidateArgs = {
  candidate?: InputMaybe<CandidateInput>;
};


export type MutationCreateJobArgs = {
  job?: InputMaybe<JobInput>;
};


export type MutationCreateOfferArgs = {
  offer?: InputMaybe<OfferInput>;
};


export type MutationCreateUserArgs = {
  user?: InputMaybe<UserInput>;
};

export type Offer = {
  __typename?: 'Offer';
  candidateEmail: Scalars['String']['output'];
  candidateId: Scalars['ID']['output'];
  candidateName?: Maybe<Scalars['String']['output']>;
  candidateSpecialization?: Maybe<Scalars['String']['output']>;
  candidateStatus: Scalars['String']['output'];
  jobDescription: Scalars['String']['output'];
  jobId: Scalars['ID']['output'];
  jobOpenPositions: Scalars['Int']['output'];
  jobTitle: Scalars['String']['output'];
  offerCreatedAt: Scalars['String']['output'];
  offerDescription: Scalars['String']['output'];
  offerId: Scalars['ID']['output'];
  offerPin: Scalars['Int']['output'];
  offerStatus: Scalars['String']['output'];
  userEmail: Scalars['String']['output'];
  userId: Scalars['ID']['output'];
  userName: Scalars['String']['output'];
};

export type OfferInput = {
  candidateId: Scalars['ID']['input'];
  description: Scalars['String']['input'];
  jobId: Scalars['ID']['input'];
  status: Scalars['String']['input'];
  userId: Scalars['ID']['input'];
};

export type OfferRaw = {
  __typename?: 'OfferRaw';
  candidateId: Scalars['ID']['output'];
  description: Scalars['String']['output'];
  id: Scalars['ID']['output'];
  jobId: Scalars['ID']['output'];
  pin?: Maybe<Scalars['Int']['output']>;
  status: Scalars['String']['output'];
  userId: Scalars['ID']['output'];
};

export type Query = {
  __typename?: 'Query';
  candidate?: Maybe<Candidate>;
  candidates?: Maybe<Array<Candidate>>;
  hello?: Maybe<Scalars['String']['output']>;
  job?: Maybe<Job>;
  jobs?: Maybe<Array<Job>>;
  offerWithPin?: Maybe<Offer>;
  user?: Maybe<User>;
  userByEmail?: Maybe<User>;
};


export type QueryCandidateArgs = {
  id: Scalars['ID']['input'];
};


export type QueryJobArgs = {
  id: Scalars['ID']['input'];
};


export type QueryOfferWithPinArgs = {
  id: Scalars['ID']['input'];
  pin: Scalars['Int']['input'];
};


export type QueryUserArgs = {
  id: Scalars['ID']['input'];
};


export type QueryUserByEmailArgs = {
  email: Scalars['String']['input'];
};

export type User = {
  __typename?: 'User';
  createdAt?: Maybe<Scalars['String']['output']>;
  email: Scalars['String']['output'];
  id: Scalars['ID']['output'];
  name?: Maybe<Scalars['String']['output']>;
  offers?: Maybe<Array<Maybe<Offer>>>;
  updatedAt?: Maybe<Scalars['String']['output']>;
};

export type UserInput = {
  email: Scalars['String']['input'];
  name?: InputMaybe<Scalars['String']['input']>;
};



export type ResolverTypeWrapper<T> = Promise<T> | T;


export type ResolverWithResolve<TResult, TParent, TContext, TArgs> = {
  resolve: ResolverFn<TResult, TParent, TContext, TArgs>;
};
export type Resolver<TResult, TParent = {}, TContext = {}, TArgs = {}> = ResolverFn<TResult, TParent, TContext, TArgs> | ResolverWithResolve<TResult, TParent, TContext, TArgs>;

export type ResolverFn<TResult, TParent, TContext, TArgs> = (
  parent: TParent,
  args: TArgs,
  context: TContext,
  info: GraphQLResolveInfo
) => Promise<TResult> | TResult;

export type SubscriptionSubscribeFn<TResult, TParent, TContext, TArgs> = (
  parent: TParent,
  args: TArgs,
  context: TContext,
  info: GraphQLResolveInfo
) => AsyncIterable<TResult> | Promise<AsyncIterable<TResult>>;

export type SubscriptionResolveFn<TResult, TParent, TContext, TArgs> = (
  parent: TParent,
  args: TArgs,
  context: TContext,
  info: GraphQLResolveInfo
) => TResult | Promise<TResult>;

export interface SubscriptionSubscriberObject<TResult, TKey extends string, TParent, TContext, TArgs> {
  subscribe: SubscriptionSubscribeFn<{ [key in TKey]: TResult }, TParent, TContext, TArgs>;
  resolve?: SubscriptionResolveFn<TResult, { [key in TKey]: TResult }, TContext, TArgs>;
}

export interface SubscriptionResolverObject<TResult, TParent, TContext, TArgs> {
  subscribe: SubscriptionSubscribeFn<any, TParent, TContext, TArgs>;
  resolve: SubscriptionResolveFn<TResult, any, TContext, TArgs>;
}

export type SubscriptionObject<TResult, TKey extends string, TParent, TContext, TArgs> =
  | SubscriptionSubscriberObject<TResult, TKey, TParent, TContext, TArgs>
  | SubscriptionResolverObject<TResult, TParent, TContext, TArgs>;

export type SubscriptionResolver<TResult, TKey extends string, TParent = {}, TContext = {}, TArgs = {}> =
  | ((...args: any[]) => SubscriptionObject<TResult, TKey, TParent, TContext, TArgs>)
  | SubscriptionObject<TResult, TKey, TParent, TContext, TArgs>;

export type TypeResolveFn<TTypes, TParent = {}, TContext = {}> = (
  parent: TParent,
  context: TContext,
  info: GraphQLResolveInfo
) => Maybe<TTypes> | Promise<Maybe<TTypes>>;

export type IsTypeOfResolverFn<T = {}, TContext = {}> = (obj: T, context: TContext, info: GraphQLResolveInfo) => boolean | Promise<boolean>;

export type NextResolverFn<T> = () => Promise<T>;

export type DirectiveResolverFn<TResult = {}, TParent = {}, TContext = {}, TArgs = {}> = (
  next: NextResolverFn<TResult>,
  parent: TParent,
  args: TArgs,
  context: TContext,
  info: GraphQLResolveInfo
) => TResult | Promise<TResult>;



/** Mapping between all available schema types and the resolvers types */
export type ResolversTypes = {
  Boolean: ResolverTypeWrapper<Scalars['Boolean']['output']>;
  Candidate: ResolverTypeWrapper<Candidate>;
  CandidateInput: CandidateInput;
  ID: ResolverTypeWrapper<Scalars['ID']['output']>;
  Int: ResolverTypeWrapper<Scalars['Int']['output']>;
  Job: ResolverTypeWrapper<Job>;
  JobInput: JobInput;
  Mutation: ResolverTypeWrapper<{}>;
  Offer: ResolverTypeWrapper<Offer>;
  OfferInput: OfferInput;
  OfferRaw: ResolverTypeWrapper<OfferRaw>;
  Query: ResolverTypeWrapper<{}>;
  String: ResolverTypeWrapper<Scalars['String']['output']>;
  User: ResolverTypeWrapper<User>;
  UserInput: UserInput;
};

/** Mapping between all available schema types and the resolvers parents */
export type ResolversParentTypes = {
  Boolean: Scalars['Boolean']['output'];
  Candidate: Candidate;
  CandidateInput: CandidateInput;
  ID: Scalars['ID']['output'];
  Int: Scalars['Int']['output'];
  Job: Job;
  JobInput: JobInput;
  Mutation: {};
  Offer: Offer;
  OfferInput: OfferInput;
  OfferRaw: OfferRaw;
  Query: {};
  String: Scalars['String']['output'];
  User: User;
  UserInput: UserInput;
};

export type AuthorisedDirectiveArgs = { };

export type AuthorisedDirectiveResolver<Result, Parent, ContextType = any, Args = AuthorisedDirectiveArgs> = DirectiveResolverFn<Result, Parent, ContextType, Args>;

export type CandidateResolvers<ContextType = any, ParentType extends ResolversParentTypes['Candidate'] = ResolversParentTypes['Candidate']> = {
  createdAt?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  email?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  id?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  name?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  specialization?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  status?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  updatedAt?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  __isTypeOf?: IsTypeOfResolverFn<ParentType, ContextType>;
};

export type JobResolvers<ContextType = any, ParentType extends ResolversParentTypes['Job'] = ResolversParentTypes['Job']> = {
  createdAt?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  description?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  id?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  openPositions?: Resolver<ResolversTypes['Int'], ParentType, ContextType>;
  title?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  updatedAt?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  __isTypeOf?: IsTypeOfResolverFn<ParentType, ContextType>;
};

export type MutationResolvers<ContextType = any, ParentType extends ResolversParentTypes['Mutation'] = ResolversParentTypes['Mutation']> = {
  createCandidate?: Resolver<Maybe<ResolversTypes['Candidate']>, ParentType, ContextType, Partial<MutationCreateCandidateArgs>>;
  createJob?: Resolver<Maybe<ResolversTypes['Job']>, ParentType, ContextType, Partial<MutationCreateJobArgs>>;
  createOffer?: Resolver<Maybe<ResolversTypes['OfferRaw']>, ParentType, ContextType, Partial<MutationCreateOfferArgs>>;
  createUser?: Resolver<Maybe<ResolversTypes['User']>, ParentType, ContextType, Partial<MutationCreateUserArgs>>;
};

export type OfferResolvers<ContextType = any, ParentType extends ResolversParentTypes['Offer'] = ResolversParentTypes['Offer']> = {
  candidateEmail?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  candidateId?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  candidateName?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  candidateSpecialization?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  candidateStatus?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  jobDescription?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  jobId?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  jobOpenPositions?: Resolver<ResolversTypes['Int'], ParentType, ContextType>;
  jobTitle?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  offerCreatedAt?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  offerDescription?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  offerId?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  offerPin?: Resolver<ResolversTypes['Int'], ParentType, ContextType>;
  offerStatus?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  userEmail?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  userId?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  userName?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  __isTypeOf?: IsTypeOfResolverFn<ParentType, ContextType>;
};

export type OfferRawResolvers<ContextType = any, ParentType extends ResolversParentTypes['OfferRaw'] = ResolversParentTypes['OfferRaw']> = {
  candidateId?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  description?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  id?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  jobId?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  pin?: Resolver<Maybe<ResolversTypes['Int']>, ParentType, ContextType>;
  status?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  userId?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  __isTypeOf?: IsTypeOfResolverFn<ParentType, ContextType>;
};

export type QueryResolvers<ContextType = any, ParentType extends ResolversParentTypes['Query'] = ResolversParentTypes['Query']> = {
  candidate?: Resolver<Maybe<ResolversTypes['Candidate']>, ParentType, ContextType, RequireFields<QueryCandidateArgs, 'id'>>;
  candidates?: Resolver<Maybe<Array<ResolversTypes['Candidate']>>, ParentType, ContextType>;
  hello?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  job?: Resolver<Maybe<ResolversTypes['Job']>, ParentType, ContextType, RequireFields<QueryJobArgs, 'id'>>;
  jobs?: Resolver<Maybe<Array<ResolversTypes['Job']>>, ParentType, ContextType>;
  offerWithPin?: Resolver<Maybe<ResolversTypes['Offer']>, ParentType, ContextType, RequireFields<QueryOfferWithPinArgs, 'id' | 'pin'>>;
  user?: Resolver<Maybe<ResolversTypes['User']>, ParentType, ContextType, RequireFields<QueryUserArgs, 'id'>>;
  userByEmail?: Resolver<Maybe<ResolversTypes['User']>, ParentType, ContextType, RequireFields<QueryUserByEmailArgs, 'email'>>;
};

export type UserResolvers<ContextType = any, ParentType extends ResolversParentTypes['User'] = ResolversParentTypes['User']> = {
  createdAt?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  email?: Resolver<ResolversTypes['String'], ParentType, ContextType>;
  id?: Resolver<ResolversTypes['ID'], ParentType, ContextType>;
  name?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  offers?: Resolver<Maybe<Array<Maybe<ResolversTypes['Offer']>>>, ParentType, ContextType>;
  updatedAt?: Resolver<Maybe<ResolversTypes['String']>, ParentType, ContextType>;
  __isTypeOf?: IsTypeOfResolverFn<ParentType, ContextType>;
};

export type Resolvers<ContextType = any> = {
  Candidate?: CandidateResolvers<ContextType>;
  Job?: JobResolvers<ContextType>;
  Mutation?: MutationResolvers<ContextType>;
  Offer?: OfferResolvers<ContextType>;
  OfferRaw?: OfferRawResolvers<ContextType>;
  Query?: QueryResolvers<ContextType>;
  User?: UserResolvers<ContextType>;
};

export type DirectiveResolvers<ContextType = any> = {
  authorised?: AuthorisedDirectiveResolver<any, any, ContextType>;
};

export type CreateCandidateMutationVariables = Exact<{
  candidate: CandidateInput;
}>;


export type CreateCandidateMutation = { __typename?: 'Mutation', createCandidate?: { __typename?: 'Candidate', id: string } | null };

export type CreateJobMutationVariables = Exact<{
  job: JobInput;
}>;


export type CreateJobMutation = { __typename?: 'Mutation', createJob?: { __typename?: 'Job', id: string } | null };

export type CreateOfferMutationVariables = Exact<{
  offer: OfferInput;
}>;


export type CreateOfferMutation = { __typename?: 'Mutation', createOffer?: { __typename?: 'OfferRaw', id: string } | null };

export type GetCandidatesQueryVariables = Exact<{ [key: string]: never; }>;


export type GetCandidatesQuery = { __typename?: 'Query', candidates?: Array<{ __typename?: 'Candidate', id: string, name?: string | null, email: string, specialization?: string | null, status: string, createdAt?: string | null, updatedAt?: string | null }> | null };

export type GetJobsQueryVariables = Exact<{ [key: string]: never; }>;


export type GetJobsQuery = { __typename?: 'Query', jobs?: Array<{ __typename?: 'Job', id: string, title: string, description: string, openPositions: number, createdAt?: string | null, updatedAt?: string | null }> | null };

export type GetOfferIngredientsQueryVariables = Exact<{ [key: string]: never; }>;


export type GetOfferIngredientsQuery = { __typename?: 'Query', jobs?: Array<{ __typename?: 'Job', id: string, title: string }> | null, candidates?: Array<{ __typename?: 'Candidate', id: string, email: string }> | null };

export type GetOfferWithPinQueryVariables = Exact<{
  pin: Scalars['Int']['input'];
  id: Scalars['ID']['input'];
}>;


export type GetOfferWithPinQuery = { __typename?: 'Query', offerWithPin?: { __typename?: 'Offer', candidateName?: string | null, candidateEmail: string, candidateSpecialization?: string | null, jobTitle: string, jobDescription: string, offerDescription: string, userName: string, userEmail: string } | null };

export type GetUserByEmailQueryVariables = Exact<{
  email: Scalars['String']['input'];
}>;


export type GetUserByEmailQuery = { __typename?: 'Query', userByEmail?: { __typename?: 'User', id: string, email: string, name?: string | null, createdAt?: string | null, updatedAt?: string | null } | null };

export type GetUserOffersQueryVariables = Exact<{
  id: Scalars['ID']['input'];
}>;


export type GetUserOffersQuery = { __typename?: 'Query', user?: { __typename?: 'User', offers?: Array<{ __typename?: 'Offer', candidateEmail: string, candidateId: string, candidateName?: string | null, offerStatus: string, offerId: string, offerPin: number, offerCreatedAt: string, jobTitle: string, jobId: string } | null> | null } | null };


export const CreateCandidateDocument = gql`
    mutation CreateCandidate($candidate: CandidateInput!) {
  createCandidate(candidate: $candidate) {
    id
  }
}
    `;
export type CreateCandidateMutationFn = Apollo.MutationFunction<CreateCandidateMutation, CreateCandidateMutationVariables>;

/**
 * __useCreateCandidateMutation__
 *
 * To run a mutation, you first call `useCreateCandidateMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateCandidateMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createCandidateMutation, { data, loading, error }] = useCreateCandidateMutation({
 *   variables: {
 *      candidate: // value for 'candidate'
 *   },
 * });
 */
export function useCreateCandidateMutation(baseOptions?: Apollo.MutationHookOptions<CreateCandidateMutation, CreateCandidateMutationVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useMutation<CreateCandidateMutation, CreateCandidateMutationVariables>(CreateCandidateDocument, options);
      }
export type CreateCandidateMutationHookResult = ReturnType<typeof useCreateCandidateMutation>;
export type CreateCandidateMutationResult = Apollo.MutationResult<CreateCandidateMutation>;
export type CreateCandidateMutationOptions = Apollo.BaseMutationOptions<CreateCandidateMutation, CreateCandidateMutationVariables>;
export const CreateJobDocument = gql`
    mutation CreateJob($job: JobInput!) {
  createJob(job: $job) {
    id
  }
}
    `;
export type CreateJobMutationFn = Apollo.MutationFunction<CreateJobMutation, CreateJobMutationVariables>;

/**
 * __useCreateJobMutation__
 *
 * To run a mutation, you first call `useCreateJobMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateJobMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createJobMutation, { data, loading, error }] = useCreateJobMutation({
 *   variables: {
 *      job: // value for 'job'
 *   },
 * });
 */
export function useCreateJobMutation(baseOptions?: Apollo.MutationHookOptions<CreateJobMutation, CreateJobMutationVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useMutation<CreateJobMutation, CreateJobMutationVariables>(CreateJobDocument, options);
      }
export type CreateJobMutationHookResult = ReturnType<typeof useCreateJobMutation>;
export type CreateJobMutationResult = Apollo.MutationResult<CreateJobMutation>;
export type CreateJobMutationOptions = Apollo.BaseMutationOptions<CreateJobMutation, CreateJobMutationVariables>;
export const CreateOfferDocument = gql`
    mutation createOffer($offer: OfferInput!) {
  createOffer(offer: $offer) {
    id
  }
}
    `;
export type CreateOfferMutationFn = Apollo.MutationFunction<CreateOfferMutation, CreateOfferMutationVariables>;

/**
 * __useCreateOfferMutation__
 *
 * To run a mutation, you first call `useCreateOfferMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateOfferMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createOfferMutation, { data, loading, error }] = useCreateOfferMutation({
 *   variables: {
 *      offer: // value for 'offer'
 *   },
 * });
 */
export function useCreateOfferMutation(baseOptions?: Apollo.MutationHookOptions<CreateOfferMutation, CreateOfferMutationVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useMutation<CreateOfferMutation, CreateOfferMutationVariables>(CreateOfferDocument, options);
      }
export type CreateOfferMutationHookResult = ReturnType<typeof useCreateOfferMutation>;
export type CreateOfferMutationResult = Apollo.MutationResult<CreateOfferMutation>;
export type CreateOfferMutationOptions = Apollo.BaseMutationOptions<CreateOfferMutation, CreateOfferMutationVariables>;
export const GetCandidatesDocument = gql`
    query GetCandidates {
  candidates {
    id
    name
    email
    specialization
    status
    createdAt
    updatedAt
  }
}
    `;

/**
 * __useGetCandidatesQuery__
 *
 * To run a query within a React component, call `useGetCandidatesQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetCandidatesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetCandidatesQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetCandidatesQuery(baseOptions?: Apollo.QueryHookOptions<GetCandidatesQuery, GetCandidatesQueryVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useQuery<GetCandidatesQuery, GetCandidatesQueryVariables>(GetCandidatesDocument, options);
      }
export function useGetCandidatesLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetCandidatesQuery, GetCandidatesQueryVariables>) {
          const options = {...defaultOptions, ...baseOptions}
          return Apollo.useLazyQuery<GetCandidatesQuery, GetCandidatesQueryVariables>(GetCandidatesDocument, options);
        }
export type GetCandidatesQueryHookResult = ReturnType<typeof useGetCandidatesQuery>;
export type GetCandidatesLazyQueryHookResult = ReturnType<typeof useGetCandidatesLazyQuery>;
export type GetCandidatesQueryResult = Apollo.QueryResult<GetCandidatesQuery, GetCandidatesQueryVariables>;
export const GetJobsDocument = gql`
    query GetJobs {
  jobs {
    id
    title
    description
    openPositions
    createdAt
    updatedAt
  }
}
    `;

/**
 * __useGetJobsQuery__
 *
 * To run a query within a React component, call `useGetJobsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetJobsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetJobsQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetJobsQuery(baseOptions?: Apollo.QueryHookOptions<GetJobsQuery, GetJobsQueryVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useQuery<GetJobsQuery, GetJobsQueryVariables>(GetJobsDocument, options);
      }
export function useGetJobsLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetJobsQuery, GetJobsQueryVariables>) {
          const options = {...defaultOptions, ...baseOptions}
          return Apollo.useLazyQuery<GetJobsQuery, GetJobsQueryVariables>(GetJobsDocument, options);
        }
export type GetJobsQueryHookResult = ReturnType<typeof useGetJobsQuery>;
export type GetJobsLazyQueryHookResult = ReturnType<typeof useGetJobsLazyQuery>;
export type GetJobsQueryResult = Apollo.QueryResult<GetJobsQuery, GetJobsQueryVariables>;
export const GetOfferIngredientsDocument = gql`
    query getOfferIngredients {
  jobs {
    id
    title
  }
  candidates {
    id
    email
  }
}
    `;

/**
 * __useGetOfferIngredientsQuery__
 *
 * To run a query within a React component, call `useGetOfferIngredientsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetOfferIngredientsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetOfferIngredientsQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetOfferIngredientsQuery(baseOptions?: Apollo.QueryHookOptions<GetOfferIngredientsQuery, GetOfferIngredientsQueryVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useQuery<GetOfferIngredientsQuery, GetOfferIngredientsQueryVariables>(GetOfferIngredientsDocument, options);
      }
export function useGetOfferIngredientsLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetOfferIngredientsQuery, GetOfferIngredientsQueryVariables>) {
          const options = {...defaultOptions, ...baseOptions}
          return Apollo.useLazyQuery<GetOfferIngredientsQuery, GetOfferIngredientsQueryVariables>(GetOfferIngredientsDocument, options);
        }
export type GetOfferIngredientsQueryHookResult = ReturnType<typeof useGetOfferIngredientsQuery>;
export type GetOfferIngredientsLazyQueryHookResult = ReturnType<typeof useGetOfferIngredientsLazyQuery>;
export type GetOfferIngredientsQueryResult = Apollo.QueryResult<GetOfferIngredientsQuery, GetOfferIngredientsQueryVariables>;
export const GetOfferWithPinDocument = gql`
    query getOfferWithPin($pin: Int!, $id: ID!) {
  offerWithPin(pin: $pin, id: $id) {
    candidateName
    candidateEmail
    candidateSpecialization
    jobTitle
    jobDescription
    offerDescription
    userName
    userEmail
  }
}
    `;

/**
 * __useGetOfferWithPinQuery__
 *
 * To run a query within a React component, call `useGetOfferWithPinQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetOfferWithPinQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetOfferWithPinQuery({
 *   variables: {
 *      pin: // value for 'pin'
 *      id: // value for 'id'
 *   },
 * });
 */
export function useGetOfferWithPinQuery(baseOptions: Apollo.QueryHookOptions<GetOfferWithPinQuery, GetOfferWithPinQueryVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useQuery<GetOfferWithPinQuery, GetOfferWithPinQueryVariables>(GetOfferWithPinDocument, options);
      }
export function useGetOfferWithPinLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetOfferWithPinQuery, GetOfferWithPinQueryVariables>) {
          const options = {...defaultOptions, ...baseOptions}
          return Apollo.useLazyQuery<GetOfferWithPinQuery, GetOfferWithPinQueryVariables>(GetOfferWithPinDocument, options);
        }
export type GetOfferWithPinQueryHookResult = ReturnType<typeof useGetOfferWithPinQuery>;
export type GetOfferWithPinLazyQueryHookResult = ReturnType<typeof useGetOfferWithPinLazyQuery>;
export type GetOfferWithPinQueryResult = Apollo.QueryResult<GetOfferWithPinQuery, GetOfferWithPinQueryVariables>;
export const GetUserByEmailDocument = gql`
    query getUserByEmail($email: String!) {
  userByEmail(email: $email) {
    id
    email
    name
    createdAt
    updatedAt
  }
}
    `;

/**
 * __useGetUserByEmailQuery__
 *
 * To run a query within a React component, call `useGetUserByEmailQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetUserByEmailQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetUserByEmailQuery({
 *   variables: {
 *      email: // value for 'email'
 *   },
 * });
 */
export function useGetUserByEmailQuery(baseOptions: Apollo.QueryHookOptions<GetUserByEmailQuery, GetUserByEmailQueryVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useQuery<GetUserByEmailQuery, GetUserByEmailQueryVariables>(GetUserByEmailDocument, options);
      }
export function useGetUserByEmailLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetUserByEmailQuery, GetUserByEmailQueryVariables>) {
          const options = {...defaultOptions, ...baseOptions}
          return Apollo.useLazyQuery<GetUserByEmailQuery, GetUserByEmailQueryVariables>(GetUserByEmailDocument, options);
        }
export type GetUserByEmailQueryHookResult = ReturnType<typeof useGetUserByEmailQuery>;
export type GetUserByEmailLazyQueryHookResult = ReturnType<typeof useGetUserByEmailLazyQuery>;
export type GetUserByEmailQueryResult = Apollo.QueryResult<GetUserByEmailQuery, GetUserByEmailQueryVariables>;
export const GetUserOffersDocument = gql`
    query getUserOffers($id: ID!) {
  user(id: $id) {
    offers {
      candidateEmail
      candidateId
      candidateName
      offerStatus
      offerId
      offerPin
      offerCreatedAt
      jobTitle
      jobId
    }
  }
}
    `;

/**
 * __useGetUserOffersQuery__
 *
 * To run a query within a React component, call `useGetUserOffersQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetUserOffersQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetUserOffersQuery({
 *   variables: {
 *      id: // value for 'id'
 *   },
 * });
 */
export function useGetUserOffersQuery(baseOptions: Apollo.QueryHookOptions<GetUserOffersQuery, GetUserOffersQueryVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useQuery<GetUserOffersQuery, GetUserOffersQueryVariables>(GetUserOffersDocument, options);
      }
export function useGetUserOffersLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetUserOffersQuery, GetUserOffersQueryVariables>) {
          const options = {...defaultOptions, ...baseOptions}
          return Apollo.useLazyQuery<GetUserOffersQuery, GetUserOffersQueryVariables>(GetUserOffersDocument, options);
        }
export type GetUserOffersQueryHookResult = ReturnType<typeof useGetUserOffersQuery>;
export type GetUserOffersLazyQueryHookResult = ReturnType<typeof useGetUserOffersLazyQuery>;
export type GetUserOffersQueryResult = Apollo.QueryResult<GetUserOffersQuery, GetUserOffersQueryVariables>;