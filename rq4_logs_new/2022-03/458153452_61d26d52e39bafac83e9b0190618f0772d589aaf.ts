import gql from 'graphql-tag';
import * as Urql from 'urql';
export type Maybe<T> = T | null;
export type InputMaybe<T> = Maybe<T>;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
export type Omit<T, K extends keyof T> = Pick<T, Exclude<keyof T, K>>;
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: string;
  String: string;
  Boolean: boolean;
  Int: number;
  Float: number;
  timestamptz: any;
  uuid: any;
};

export type AbortMultipartUploadReturn = {
  __typename?: 'AbortMultipartUploadReturn';
  message: Scalars['String'];
};

/** Boolean expression to compare columns of type "Boolean". All fields are combined with logical 'AND'. */
export type Boolean_Comparison_Exp = {
  _eq?: InputMaybe<Scalars['Boolean']>;
  _gt?: InputMaybe<Scalars['Boolean']>;
  _gte?: InputMaybe<Scalars['Boolean']>;
  _in?: InputMaybe<Array<Scalars['Boolean']>>;
  _is_null?: InputMaybe<Scalars['Boolean']>;
  _lt?: InputMaybe<Scalars['Boolean']>;
  _lte?: InputMaybe<Scalars['Boolean']>;
  _neq?: InputMaybe<Scalars['Boolean']>;
  _nin?: InputMaybe<Array<Scalars['Boolean']>>;
};

export type CompleteMultipartUploadReturn = {
  __typename?: 'CompleteMultipartUploadReturn';
  location: Scalars['String'];
};

export type CreateMultipartUploadReturn = {
  __typename?: 'CreateMultipartUploadReturn';
  key: Scalars['String'];
  uploadId: Scalars['String'];
};

export type File = {
  __typename?: 'File';
  id?: Maybe<Scalars['ID']>;
  numParts?: Maybe<Scalars['Int']>;
  path?: Maybe<Scalars['String']>;
  uploadId?: Maybe<Scalars['String']>;
};

/** Boolean expression to compare columns of type "Float". All fields are combined with logical 'AND'. */
export type Float_Comparison_Exp = {
  _eq?: InputMaybe<Scalars['Float']>;
  _gt?: InputMaybe<Scalars['Float']>;
  _gte?: InputMaybe<Scalars['Float']>;
  _in?: InputMaybe<Array<Scalars['Float']>>;
  _is_null?: InputMaybe<Scalars['Boolean']>;
  _lt?: InputMaybe<Scalars['Float']>;
  _lte?: InputMaybe<Scalars['Float']>;
  _neq?: InputMaybe<Scalars['Float']>;
  _nin?: InputMaybe<Array<Scalars['Float']>>;
};

export type GetUrlReturn = {
  __typename?: 'GetUrlReturn';
  url: Scalars['String'];
};

export type GetUrlsReturn = {
  __typename?: 'GetUrlsReturn';
  urls: Array<Maybe<Scalars['String']>>;
};

/** Boolean expression to compare columns of type "Int". All fields are combined with logical 'AND'. */
export type Int_Comparison_Exp = {
  _eq?: InputMaybe<Scalars['Int']>;
  _gt?: InputMaybe<Scalars['Int']>;
  _gte?: InputMaybe<Scalars['Int']>;
  _in?: InputMaybe<Array<Scalars['Int']>>;
  _is_null?: InputMaybe<Scalars['Boolean']>;
  _lt?: InputMaybe<Scalars['Int']>;
  _lte?: InputMaybe<Scalars['Int']>;
  _neq?: InputMaybe<Scalars['Int']>;
  _nin?: InputMaybe<Array<Scalars['Int']>>;
};

export type Part = {
  __typename?: 'Part';
  ETag?: Maybe<Scalars['String']>;
  PartNumber?: Maybe<Scalars['Int']>;
  size?: Maybe<Scalars['Int']>;
};

export type PartInput = {
  ETag?: InputMaybe<Scalars['String']>;
  PartNumber?: InputMaybe<Scalars['Int']>;
  size?: InputMaybe<Scalars['Int']>;
};

/** Boolean expression to compare columns of type "String". All fields are combined with logical 'AND'. */
export type String_Comparison_Exp = {
  _eq?: InputMaybe<Scalars['String']>;
  _gt?: InputMaybe<Scalars['String']>;
  _gte?: InputMaybe<Scalars['String']>;
  /** does the column match the given case-insensitive pattern */
  _ilike?: InputMaybe<Scalars['String']>;
  _in?: InputMaybe<Array<Scalars['String']>>;
  /** does the column match the given POSIX regular expression, case insensitive */
  _iregex?: InputMaybe<Scalars['String']>;
  _is_null?: InputMaybe<Scalars['Boolean']>;
  /** does the column match the given pattern */
  _like?: InputMaybe<Scalars['String']>;
  _lt?: InputMaybe<Scalars['String']>;
  _lte?: InputMaybe<Scalars['String']>;
  _neq?: InputMaybe<Scalars['String']>;
  /** does the column NOT match the given case-insensitive pattern */
  _nilike?: InputMaybe<Scalars['String']>;
  _nin?: InputMaybe<Array<Scalars['String']>>;
  /** does the column NOT match the given POSIX regular expression, case insensitive */
  _niregex?: InputMaybe<Scalars['String']>;
  /** does the column NOT match the given pattern */
  _nlike?: InputMaybe<Scalars['String']>;
  /** does the column NOT match the given POSIX regular expression, case sensitive */
  _nregex?: InputMaybe<Scalars['String']>;
  /** does the column NOT match the given SQL regular expression */
  _nsimilar?: InputMaybe<Scalars['String']>;
  /** does the column match the given POSIX regular expression, case sensitive */
  _regex?: InputMaybe<Scalars['String']>;
  /** does the column match the given SQL regular expression */
  _similar?: InputMaybe<Scalars['String']>;
};




/** mutation root */
export type Mutation_Root = {
  __typename?: 'mutation_root';
  abortMultipartUpload?: Maybe<AbortMultipartUploadReturn>;
  completeMultipartUpload?: Maybe<CompleteMultipartUploadReturn>;
  createMultipartUpload?: Maybe<CreateMultipartUploadReturn>;
};


/** mutation root */
export type Mutation_RootAbortMultipartUploadArgs = {
  fileKey: Scalars['String'];
  uploadId: Scalars['String'];
};


/** mutation root */
export type Mutation_RootCompleteMultipartUploadArgs = {
  fileKey: Scalars['String'];
  parts?: InputMaybe<Array<InputMaybe<PartInput>>>;
  uploadId: Scalars['String'];
};


/** mutation root */
export type Mutation_RootCreateMultipartUploadArgs = {
  fileKey: Scalars['String'];
  metadata?: InputMaybe<Scalars['String']>;
  numParts?: InputMaybe<Scalars['Int']>;
};





/** column ordering options */
export enum Order_By {
  /** in ascending order, nulls last */
  Asc = 'asc',
  /** in ascending order, nulls first */
  AscNullsFirst = 'asc_nulls_first',
  /** in ascending order, nulls last */
  AscNullsLast = 'asc_nulls_last',
  /** in descending order, nulls first */
  Desc = 'desc',
  /** in descending order, nulls first */
  DescNullsFirst = 'desc_nulls_first',
  /** in descending order, nulls last */
  DescNullsLast = 'desc_nulls_last'
}

export type Query_Root = {
  __typename?: 'query_root';

  downloadGetUrl?: Maybe<GetUrlReturn>;
  downloadGetUrls?: Maybe<GetUrlsReturn>;
  file?: Maybe<File>;

  listParts?: Maybe<Part>;
  part?: Maybe<Part>;
  prepareUploadParts?: Maybe<GetUrlReturn>;
  /** fetch data from the table: "sensors" using primary key columns */
  
};













export type Query_RootDownloadGetUrlArgs = {
  fileKey: Scalars['String'];
};


export type Query_RootDownloadGetUrlsArgs = {
  fileKeys: Array<InputMaybe<Scalars['String']>>;
};




export type Query_RootListPartsArgs = {
  fileKey: Scalars['String'];
  uploadId: Scalars['String'];
};


export type Query_RootPrepareUploadPartsArgs = {
  fileKey: Scalars['String'];
  partNumber: Scalars['Int'];
  uploadId: Scalars['String'];
};











export type DownloadGetUrlQueryVariables = Exact<{
  fileKey: Scalars['String'];
}>;


export type DownloadGetUrlQuery = { __typename?: 'query_root', downloadGetUrl?: { __typename?: 'GetUrlReturn', url: string } | null };

export type DownloadGetUrlsQueryVariables = Exact<{
  fileKeys: Array<InputMaybe<Scalars['String']>> | InputMaybe<Scalars['String']>;
}>;


export type DownloadGetUrlsQuery = { __typename?: 'query_root', downloadGetUrls?: { __typename?: 'GetUrlsReturn', urls: Array<string | null> } | null };

export type CreateMultipartUploadMutationVariables = Exact<{
  fileKey: Scalars['String'];
}>;


export type CreateMultipartUploadMutation = { __typename?: 'mutation_root', createMultipartUpload?: { __typename?: 'CreateMultipartUploadReturn', uploadId: string, key: string } | null };

export type PrepareUploadPartsQueryVariables = Exact<{
  fileKey: Scalars['String'];
  uploadId: Scalars['String'];
  partNumber: Scalars['Int'];
}>;


export type PrepareUploadPartsQuery = { __typename?: 'query_root', prepareUploadParts?: { __typename?: 'GetUrlReturn', url: string } | null };

export type ListPartsQueryVariables = Exact<{
  fileKey: Scalars['String'];
  uploadId: Scalars['String'];
}>;


export type ListPartsQuery = { __typename?: 'query_root', listParts?: { __typename?: 'Part', ETag?: string | null, size?: number | null, PartNumber?: number | null } | null };

export type AbortMultipartUploadMutationVariables = Exact<{
  fileKey: Scalars['String'];
  uploadId: Scalars['String'];
}>;


export type AbortMultipartUploadMutation = { __typename?: 'mutation_root', abortMultipartUpload?: { __typename?: 'AbortMultipartUploadReturn', message: string } | null };

export type CompleteMultipartUploadMutationVariables = Exact<{
  fileKey: Scalars['String'];
  uploadId: Scalars['String'];
  parts: Array<InputMaybe<PartInput>> | InputMaybe<PartInput>;
}>;


export type CompleteMultipartUploadMutation = { __typename?: 'mutation_root', completeMultipartUpload?: { __typename?: 'CompleteMultipartUploadReturn', location: string } | null };


export const DownloadGetUrlDocument = gql`
    query downloadGetUrl($fileKey: String!) {
  downloadGetUrl(fileKey: $fileKey) {
    url
  }
}
    `;

export function useDownloadGetUrlQuery(options: Omit<Urql.UseQueryArgs<DownloadGetUrlQueryVariables>, 'query'>) {
  return Urql.useQuery<DownloadGetUrlQuery>({ query: DownloadGetUrlDocument, ...options });
};
export const DownloadGetUrlsDocument = gql`
    query downloadGetUrls($fileKeys: [String]!) {
  downloadGetUrls(fileKeys: $fileKeys) {
    urls
  }
}
    `;

export function useDownloadGetUrlsQuery(options: Omit<Urql.UseQueryArgs<DownloadGetUrlsQueryVariables>, 'query'>) {
  return Urql.useQuery<DownloadGetUrlsQuery>({ query: DownloadGetUrlsDocument, ...options });
};
export const CreateMultipartUploadDocument = gql`
    mutation createMultipartUpload($fileKey: String!) {
  createMultipartUpload(fileKey: $fileKey) {
    uploadId
    key
  }
}
    `;

export function useCreateMultipartUploadMutation() {
  return Urql.useMutation<CreateMultipartUploadMutation, CreateMultipartUploadMutationVariables>(CreateMultipartUploadDocument);
};
export const PrepareUploadPartsDocument = gql`
    query prepareUploadParts($fileKey: String!, $uploadId: String!, $partNumber: Int!) {
  prepareUploadParts(
    fileKey: $fileKey
    uploadId: $uploadId
    partNumber: $partNumber
  ) {
    url
  }
}
    `;

export function usePrepareUploadPartsQuery(options: Omit<Urql.UseQueryArgs<PrepareUploadPartsQueryVariables>, 'query'>) {
  return Urql.useQuery<PrepareUploadPartsQuery>({ query: PrepareUploadPartsDocument, ...options });
};
export const ListPartsDocument = gql`
    query listParts($fileKey: String!, $uploadId: String!) {
  listParts(fileKey: $fileKey, uploadId: $uploadId) {
    ETag
    size
    PartNumber
  }
}
    `;

export function useListPartsQuery(options: Omit<Urql.UseQueryArgs<ListPartsQueryVariables>, 'query'>) {
  return Urql.useQuery<ListPartsQuery>({ query: ListPartsDocument, ...options });
};
export const AbortMultipartUploadDocument = gql`
    mutation abortMultipartUpload($fileKey: String!, $uploadId: String!) {
  abortMultipartUpload(fileKey: $fileKey, uploadId: $uploadId) {
    message
  }
}
    `;

export function useAbortMultipartUploadMutation() {
  return Urql.useMutation<AbortMultipartUploadMutation, AbortMultipartUploadMutationVariables>(AbortMultipartUploadDocument);
};
export const CompleteMultipartUploadDocument = gql`
    mutation completeMultipartUpload($fileKey: String!, $uploadId: String!, $parts: [PartInput]!) {
  completeMultipartUpload(fileKey: $fileKey, uploadId: $uploadId, parts: $parts) {
    location
  }
}
    `;

export function useCompleteMultipartUploadMutation() {
  return Urql.useMutation<CompleteMultipartUploadMutation, CompleteMultipartUploadMutationVariables>(CompleteMultipartUploadDocument);
};