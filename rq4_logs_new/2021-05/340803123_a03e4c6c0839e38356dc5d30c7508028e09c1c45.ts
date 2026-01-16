import {
  AddMembersToOrganizationMutation,
  CreateOrganizationMutation,
  GetCurrentUserInfoQuery,
  MutationAddMembersToOrganizationArgs,
  MutationCreateOrganizationArgs,
  SearchUserQuery,
  SearchUserQueryVariables
} from "@/apollo/types";
import { useMutation, useQuery } from "@vue/apollo-composable";
import gql from "graphql-tag";

export const useUserInfo = () =>
  useQuery<GetCurrentUserInfoQuery>(
    gql`
      query getCurrentUserInfo {
        currentUser {
          id
          firstName
          lastName
          email
          phoneNumber
        }
      }
    `
  );

export const useSearchUser = (variables: SearchUserQueryVariables) =>
  useQuery<SearchUserQuery>(
    gql`
      query searchUser($input: String!) {
        searchUser(keyword: $input) {
          id
          profilePictureUrl
          firstName
          lastName
          email
        }
      }
    `,
    variables
  );

export const useCreateOrganization = () => {
  const { mutate: createOrganization, onDone } = useMutation<
    CreateOrganizationMutation,
    MutationCreateOrganizationArgs
  >(gql`
    mutation createOrganization($input: CreateOrganizationInput!) {
      createOrganization(input: $input) {
        id
      }
    }
  `);
  return { createOrganization, onDone };
};

export const useAddMemberToOrg = () => {
  const { mutate: addMember } = useMutation<
    AddMembersToOrganizationMutation,
    MutationAddMembersToOrganizationArgs
  >(gql`
    mutation addMembersToOrganization(
      $input: UpdateMembersInOrganizationInput!
    ) {
      addMembersToOrganization(input: $input)
    }
  `);
  return { addMember };
};