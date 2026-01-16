import { gql, useQuery } from "@apollo/client"
import { Repository } from "../../types"

interface RepositoriesResponse {
  repositories: {
    edges: ReadonlyArray<{
      node: Repository
    }>
  }
}

const REPOSITORIES = gql`
  query {
    repositories {
      edges {
        node {
          id
          fullName
          description
          language
          forksCount
          stargazersCount
          ratingAverage
          reviewCount
          ownerAvatarUrl
        }
      }
    }
  }
`

export const useRepositories = () => {
  const { data, loading, refetch } = useQuery<RepositoriesResponse>(REPOSITORIES, {
    fetchPolicy: "cache-and-network"
  })
  const repositories = !data
    ? []
    : data.repositories.edges.map(edge => edge.node)

  return {
    repositories,
    loading,
    refetch
  }
}