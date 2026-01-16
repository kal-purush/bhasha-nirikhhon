import { ApolloClient } from "apollo-client";
import { InMemoryCache } from "apollo-cache-inmemory";
import { HttpLink } from "apollo-link-http";
import { Operation, ApolloLink, Observable } from "apollo-link";
import { withClientState } from "apollo-link-state";

const cache = new InMemoryCache();
const link = new HttpLink({
  uri: "http://localhost:4000/graphql",
  credentials: "include"
});

const request = async (operation: Operation) => {
  operation.setContext({
    headers: {
      token: localStorage.getItem("token") || ""
    }
  });
};

const requestLink = new ApolloLink(
  (operation, forward) =>
    new Observable(observer => {
      let handle: any;
      Promise.resolve(operation)
        .then(oper => request(oper))
        .then(() => {
          handle = forward(operation).subscribe({
            next: observer.next.bind(observer),
            error: observer.error.bind(observer),
            complete: observer.complete.bind(observer)
          });
        })
        .catch(observer.error.bind(observer));

      return () => {
        if (handle) handle.unsubscribe();
      };
    })
);

const clientState = withClientState({
  defaults: {
    auth: {
      __typename: "Auth",
      isLoggedIn: Boolean(localStorage.getItem("token"))
    }
  },
  resolvers: {
    Mutation: {
      logUserOut: (_, __, { cache }) => {
        localStorage.removeItem("token");
        cache.writeData({
          data: {
            __typename: "Auth",
            isLoggedIn: false
          }
        });

        return null;
      },
      logUserIn: (_, { token }, { cache }) => {
        localStorage.setItem("token", token);
        cache.writeData({
          data: {
            __typename: "Auth",
            isLoggedIn: true
          }
        });
        return null;
      }
    }
  },
  cache
});

const client = new ApolloClient({
  link: ApolloLink.from([requestLink, link, clientState]),
  cache
});

export default client;