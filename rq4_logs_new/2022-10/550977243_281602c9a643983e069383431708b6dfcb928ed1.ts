import { gql } from "graphql-request";
import {
  GetPostDetailsQuery,
  GetPostDetailsQueryVariables,
} from "../generated-graphql/graphql";
import { requestClient } from "../graphql/graphqlRequest";

const GET_POST_DETAILS = gql`
  query GetPostDetails($slug: String!) {
    post(where: { slug: $slug }) {
      author {
        bio
        name
        id
        photo {
          url
        }
      }
      createdAt
      slug
      title
      excerpt
      featuredImage {
        url
      }
      categhories {
        name
        slug
      }
      content {
        markdown
      }
    }
  }
`;

export const getPostDetails = async (slug: string) => {
  const data = await requestClient.request<
    GetPostDetailsQuery,
    GetPostDetailsQueryVariables
  >(GET_POST_DETAILS, { slug });

  return data.post;
};