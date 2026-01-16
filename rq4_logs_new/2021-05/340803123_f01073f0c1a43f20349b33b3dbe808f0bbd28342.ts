import {
  GetSearchedItemsQuery,
  GetSearchedItemsQueryVariables
} from "@/apollo/types";
import { useQuery } from "@vue/apollo-composable";
import gql from "graphql-tag";

export const useSearchedItems = (variables: GetSearchedItemsQueryVariables) =>
  useQuery<GetSearchedItemsQuery>(
    gql`
      query getSearchedItems($keyword: String!) {
        searchEvent(keyword: $keyword) {
          id
          description
          name
          coverImageUrl
          coverImageHash
          posterImageUrl
          posterImageHash
          durations {
            id
            start
            finish
          }
          location {
            id
            name
          }
          tags {
            id
            name
          }
        }
      }
    `,
    variables
  );