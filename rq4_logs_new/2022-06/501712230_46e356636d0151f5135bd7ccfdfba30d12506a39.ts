import {
  ApolloClient,
  HttpLink,
  InMemoryCache,
  gql,
} from "@apollo/client/core";
import fetch from "cross-fetch";

const client = new ApolloClient({
  link: new HttpLink({
    uri: "http://backend:4000/",
    fetch,
  }),
  cache: new InMemoryCache(),
});

describe("book resolver", () => {
  it("books are empty", async () => {
    const res = await client.query({
      query: gql`
        query Books {
          books {
            title
            author
          }
        }
      `,
    });
    expect(res.data?.books).toEqual([]);
  });
});