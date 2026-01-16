import { InMemoryCache } from "apollo-cache-inmemory";
import { ApolloLink } from "apollo-link";
import { client } from "./apollo";

describe("Apollo Client", (): void => {
  test("Has a link", (): void => {
    expect(client.link).toBeDefined();
  });
  test("Has a cache", (): void => {
    expect(client.cache).toBeDefined();
  });
});

describe("Client Link", (): void => {
  test("is an ApolloLink", (): void => {
    expect(client.link).toBeInstanceOf(ApolloLink);
  });
});

describe("Client Cache", (): void => {
  test("is an InMemoryCache", (): void => {
    expect(client.cache).toBeInstanceOf(InMemoryCache);
  });
});