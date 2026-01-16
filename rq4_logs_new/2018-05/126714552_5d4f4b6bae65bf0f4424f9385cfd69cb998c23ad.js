import { ApolloLink, concat, split } from "apollo-link";
import { HttpLink } from "apollo-link-http";
import { InMemoryCache } from "apollo-cache-inmemory";
import { WebSocketLink } from "apollo-link-ws";
import { getMainDefinition } from "apollo-utilities";
import "subscriptions-transport-ws"; // this is the default of apollo-link-ws

var cookieparser = require("cookieparser");
let alias = "cj3cl5pn6gfwq015999r6ookh";

export default ctx => {
  var parsed = process.server ? cookieparser.parse(ctx.req.headers.cookie) : "";

  const httpLink = new HttpLink({
    uri: "https://api.graph.cool/simple/v1/" + alias
  });

  // auth token
  let token = process.server
    ? parsed["auth._token.auth0"]
    : localStorage.getItem("auth._token.auth0");

  // middleware
  const middlewareLink = new ApolloLink((operation, forward) => {
    operation.setContext({
      headers: { Authorization: token }
    });
    return forward(operation);
  });

  const wsLink = process.client
    ? new WebSocketLink({
        uri: `wss://subscriptions.graph.cool/v1/${alias}`,
        options: {
          reconnect: true,
          connectionParams: () => {
            return {
              Authorization: token
            };
          }
        }
      })
    : "";

  const link = process.server
    ? httpLink
    : split(
        ({ query }) => {
          const { kind, operation } = getMainDefinition(query);
          return kind === "OperationDefinition" && operation === "subscription";
        },
        wsLink,
        httpLink
      );

  return {
    link: concat(middlewareLink, link),
    cache: new InMemoryCache()
  };
};