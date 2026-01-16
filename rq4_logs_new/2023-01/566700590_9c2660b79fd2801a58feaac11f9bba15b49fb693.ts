import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type Maybe<T> = T | null;
export type InputMaybe<T> = Maybe<T>;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
const defaultOptions = {} as const;
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: string;
  String: string;
  Boolean: boolean;
  Int: number;
  Float: number;
};

export type Category = {
  __typename?: 'Category';
  id: Scalars['Float'];
  name: Scalars['String'];
  picto: Scalars['String'];
};

export type CategoryInput = {
  name: Scalars['String'];
  picto: Scalars['String'];
};

export type City = {
  __typename?: 'City';
  description: Scalars['String'];
  id: Scalars['Float'];
  latitude: Scalars['String'];
  longitude: Scalars['String'];
  name: Scalars['String'];
  picture: Scalars['String'];
};

export type CityInput = {
  description: Scalars['String'];
  latitude: Scalars['String'];
  longitude: Scalars['String'];
  name: Scalars['String'];
  picture: Scalars['String'];
};

export type Mutation = {
  __typename?: 'Mutation';
  createCategory: Category;
  createCity: City;
  createPlace: Place;
  updateCategory: Category;
  updateCity: City;
  updatePlace: Place;
};


export type MutationCreateCategoryArgs = {
  data: CategoryInput;
};


export type MutationCreateCityArgs = {
  data: CityInput;
};


export type MutationCreatePlaceArgs = {
  data: PlaceInput;
};


export type MutationUpdateCategoryArgs = {
  data: CategoryInput;
  id: Scalars['Int'];
};


export type MutationUpdateCityArgs = {
  data: CityInput;
  id: Scalars['String'];
};


export type MutationUpdatePlaceArgs = {
  data: PlaceInput;
  id: Scalars['String'];
};

export type Place = {
  __typename?: 'Place';
  adress: Scalars['String'];
  categoryId: Scalars['Float'];
  cityId: Scalars['Float'];
  description: Scalars['String'];
  id: Scalars['Float'];
  latitude: Scalars['String'];
  longitude: Scalars['String'];
  name: Scalars['String'];
  picture: Scalars['String'];
  website?: Maybe<Scalars['String']>;
};

export type PlaceInput = {
  adress: Scalars['String'];
  categoryId: Scalars['Float'];
  cityId: Scalars['Float'];
  description: Scalars['String'];
  latitude: Scalars['String'];
  longitude: Scalars['String'];
  name: Scalars['String'];
  picture: Scalars['String'];
  website?: InputMaybe<Scalars['String']>;
};

export type Query = {
  __typename?: 'Query';
  getCategories: Array<Category>;
  getCities: Array<City>;
  getOneCitybyId: City;
  getOnePlacebyId: Place;
  getPlaces: Array<Place>;
};


export type QueryGetOneCitybyIdArgs = {
  id: Scalars['String'];
};


export type QueryGetOnePlacebyIdArgs = {
  id: Scalars['String'];
};

export type GetCitiesQueryVariables = Exact<{ [key: string]: never; }>;


export type GetCitiesQuery = { __typename?: 'Query', getCities: Array<{ __typename?: 'City', id: number, name: string, picture: string }> };


export const GetCitiesDocument = gql`
    query GetCities {
  getCities {
    id
    name
    picture
  }
}
    `;

/**
 * __useGetCitiesQuery__
 *
 * To run a query within a React component, call `useGetCitiesQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetCitiesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetCitiesQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetCitiesQuery(baseOptions?: Apollo.QueryHookOptions<GetCitiesQuery, GetCitiesQueryVariables>) {
        const options = {...defaultOptions, ...baseOptions}
        return Apollo.useQuery<GetCitiesQuery, GetCitiesQueryVariables>(GetCitiesDocument, options);
      }
export function useGetCitiesLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetCitiesQuery, GetCitiesQueryVariables>) {
          const options = {...defaultOptions, ...baseOptions}
          return Apollo.useLazyQuery<GetCitiesQuery, GetCitiesQueryVariables>(GetCitiesDocument, options);
        }
export type GetCitiesQueryHookResult = ReturnType<typeof useGetCitiesQuery>;
export type GetCitiesLazyQueryHookResult = ReturnType<typeof useGetCitiesLazyQuery>;
export type GetCitiesQueryResult = Apollo.QueryResult<GetCitiesQuery, GetCitiesQueryVariables>;