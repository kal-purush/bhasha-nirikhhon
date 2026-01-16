import React from 'react'
import { useApolloClient, OperationVariables, DocumentNode, useMutation } from '@apollo/client'
import { from } from 'rxjs'

/**
 * @description This function is used to lazily query a GraphQL API
 * @param query GraphQL Query Object
 */
export function useLazyQuery$<TData = any, TVariables = OperationVariables>(query: DocumentNode) {
  const client = useApolloClient()
  const obs = (variables: TVariables) => from(client.query({ query, variables }))
  return React.useCallback((variables: TVariables) => {
    return obs(variables)
  }, [client])
}

export function useMutation$<TData = any, TVariables = OperationVariables>(query: DocumentNode) {
  const [mutation] = useMutation(query)
  const obs = (variables: TVariables) => from(mutation({ variables }))
  return React.useCallback((variables: TVariables) => {
    return obs(variables)
  }, [mutation])
}