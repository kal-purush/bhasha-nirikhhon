import { useQuery } from "@vue/apollo-composable";
import { GetOrgTeamItemQuery } from "@/apollo/types";
import gql from "graphql-tag";

//Change later to be dynamic id
export const useOrgTeamApi = () =>
  useQuery<GetOrgTeamItemQuery>(gql`
    query getOrgTeamItem {
      organization(id: 5) {
        id
        name
        abbreviation
        description
        profilePictureUrl
        profilePictureHash
        events {
          id
          name
          posterImageUrl
          posterImageHash
          durations {
            start
            finish
          }
          registrationDueDate
          location {
            name
          }
          attendeeCount
          attendeeLimit
          attendees {
            id
          }
        }
      }
    }
  `);