import { gql } from "graphql-request";

import { GetRecentedPostsQuery } from "../generated-graphql/graphql";
import { requestClient } from "../graphql/graphqlRequest";

export const GET_RECENT_POSTS = gql`
  query GetPostDetails {
    posts(orderBy: createdAt_ASC, last: 3) {
      title
      featuredImage {
        url
      }
      createdAt
      slug
    }
  }
`;

export const getRecentPosts = async () => {
  const data = await requestClient.request<GetRecentedPostsQuery>(
    GET_RECENT_POSTS
  );

  return data.posts;
};