export type Maybe<T> = T | null;
export type InputMaybe<T> = Maybe<T>;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
export type MakeEmpty<T extends { [key: string]: unknown }, K extends keyof T> = { [_ in K]?: never };
export type Incremental<T> = T | { [P in keyof T]?: P extends ' $fragmentName' | '__typename' ? T[P] : never };
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: { input: string; output: string; }
  String: { input: string; output: string; }
  Boolean: { input: boolean; output: boolean; }
  Int: { input: number; output: number; }
  Float: { input: number; output: number; }
};

export type Competency = {
  __typename?: 'Competency';
  advantages?: Maybe<Array<Maybe<Scalars['String']['output']>>>;
  appelations?: Maybe<Array<Maybe<Scalars['String']['output']>>>;
  conclusion?: Maybe<Scalars['String']['output']>;
  definition?: Maybe<Scalars['String']['output']>;
  development?: Maybe<Scalars['String']['output']>;
  examples?: Maybe<Array<Maybe<Scalars['String']['output']>>>;
  extension?: Maybe<Scalars['String']['output']>;
  id?: Maybe<Scalars['ID']['output']>;
  importance?: Maybe<Scalars['String']['output']>;
  keywords?: Maybe<Array<Maybe<Scalars['String']['output']>>>;
  relatedSkills?: Maybe<Array<Maybe<Scalars['String']['output']>>>;
  slug?: Maybe<Scalars['String']['output']>;
  title?: Maybe<Scalars['String']['output']>;
};

export type CompetencyInput = {
  title?: InputMaybe<Scalars['String']['input']>;
};

export type PageParamsInput = {
  skip?: InputMaybe<Scalars['Int']['input']>;
  take?: InputMaybe<Scalars['Int']['input']>;
};

export type Query = {
  __typename?: 'Query';
  competencies?: Maybe<Array<Maybe<Competency>>>;
  oneCompetency?: Maybe<Competency>;
};


export type QueryCompetenciesArgs = {
  data: CompetencyInput;
  includeDefinition?: InputMaybe<Scalars['Boolean']['input']>;
  params?: InputMaybe<PageParamsInput>;
};


export type QueryOneCompetencyArgs = {
  data: CompetencyInput;
  includeDefinition?: InputMaybe<Scalars['Boolean']['input']>;
};

export type GetManyCompetenciesQueryVariables = Exact<{
  data: CompetencyInput;
  params?: InputMaybe<PageParamsInput>;
  includeDefinition?: InputMaybe<Scalars['Boolean']['input']>;
}>;


export type GetManyCompetenciesQuery = { __typename?: 'Query', competencies?: Array<{ __typename?: 'Competency', title?: string | null, definition?: string | null, relatedSkills?: Array<string | null> | null, advantages?: Array<string | null> | null, examples?: Array<string | null> | null, importance?: string | null, development?: string | null, keywords?: Array<string | null> | null, conclusion?: string | null, extension?: string | null, slug?: string | null } | null> | null };

export type GetOneCompetencyQueryVariables = Exact<{
  data: CompetencyInput;
  includeDefinition?: InputMaybe<Scalars['Boolean']['input']>;
}>;


export type GetOneCompetencyQuery = { __typename?: 'Query', oneCompetency?: { __typename?: 'Competency', title?: string | null, definition?: string | null, relatedSkills?: Array<string | null> | null, advantages?: Array<string | null> | null, examples?: Array<string | null> | null, importance?: string | null, development?: string | null, keywords?: Array<string | null> | null, conclusion?: string | null, extension?: string | null, slug?: string | null } | null };