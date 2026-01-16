import { useQuery } from "@vue/apollo-composable";
import gql from "graphql-tag";
import { GetUserProfileItemQuery } from "@/apollo/types";

export const useUserProfileApi = () =>
  useQuery<GetUserProfileItemQuery>(
    gql`
      query getUserProfileItem {
        currentUser {
          id
          profilePictureUrl
          gender
          academicYear
          email
          phoneNumber
          district
          province
          zipCode
          address
        }
      }
    `
  );