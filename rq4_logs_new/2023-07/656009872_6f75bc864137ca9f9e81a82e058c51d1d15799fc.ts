import { gql } from 'graphql-request';

export const COMMIT_HISTORY_QUERY = gql`
  query CommitHistory(
    $owner: String!
    $name: String!
    $branch: String!
    $path: String!
  ) {
    repository(owner: $owner, name: $name) {
      info: ref(qualifiedName: $branch) {
        target {
          ... on Commit {
            history(first: 1, path: $path) {
              nodes {
                committedDate
                messageHeadline
              }
            }
          }
        }
      }
    }
  }
`;