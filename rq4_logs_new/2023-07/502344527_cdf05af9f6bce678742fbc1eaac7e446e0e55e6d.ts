import { HttpLink } from "@apollo/client";
import { registerApolloClient } from "@apollo/experimental-nextjs-app-support/rsc";
import {
  NextSSRApolloClient,
  NextSSRInMemoryCache,
} from "@apollo/experimental-nextjs-app-support/ssr";
export const { getClient } = registerApolloClient(() => {
  const { GRAPHQL_ENDPOINT, API_TOKEN } = process.env;
  console.log(GRAPHQL_ENDPOINT, API_TOKEN);
  return new NextSSRApolloClient({
    uri: process.env.GRAPHQL_ENDPOINT,
    credentials: "same-origin",
    headers: {
      Authorization: `Bearer ${process.env.API_TOKEN}`,
    },
    cache: new NextSSRInMemoryCache(),
    link: new HttpLink({
      uri: process.env.GRAPHQL_ENDPOINT,
      credentials: "same-origin",
      headers: {
        Authorization: `Bearer ${process.env.API_TOKEN}`,
      },
    }),
  });
});