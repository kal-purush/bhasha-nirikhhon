"use server";

import { gql } from "@apollo/client";
import { currentUser } from "@clerk/nextjs/server";

import { createApolloClient } from "@/lib/apolloClient";

export async function getUser() {
  const client = await createApolloClient();
  const user = await currentUser();

  const userId = user?.privateMetadata["userId"];

  if (!userId) {
    return null;
  }

  try {
    const { data } = await client.query({
      query: gql`
        query FindUser($input: FindUserInput!) {
          user(input: $input) {
            id
            email
            name
            monthlyIncome
            totalMoney
            lastMonthTotalMoney
            payday
          }
        }
      `,
      variables: {
        input: {
          id: userId,
        },
      },
    });

    return data.user;
  } catch (error) {
    console.log(error);
  }
}

export async function getGoals() {
  const client = await createApolloClient();
  const user = await currentUser();

  const userId = user?.privateMetadata["userId"];

  if (!userId) {
    return null;
  }

  try {
    const { data } = await client.query({
      query: gql`
        query FindGoals($input: FindUserInput!) {
          goals(input: $input) {
            id
            email
            name
            monthlyIncome
            totalMoney
            payday
          }
        }
      `,
      variables: {
        input: {
          id: userId,
        },
      },
    });

    return data.user;
  } catch (error) {
    console.log(error);
  }
}