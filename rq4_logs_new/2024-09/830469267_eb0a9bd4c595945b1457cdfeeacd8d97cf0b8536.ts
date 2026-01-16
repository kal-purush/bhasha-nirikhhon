import { ApolloClient, InMemoryCache } from "@apollo/client";
import { GraphQLWsLink } from "@apollo/client/link/subscriptions";
import { createClient } from "graphql-ws";

import { WEBSOCKET_URL } from "../configs/urls";
import { User } from "../lib/types";

const useGraphQL = (user: User | undefined) => {
    const wsLink = new GraphQLWsLink(
        createClient({
            url: WEBSOCKET_URL,
            connectionParams: { authToken: user?.token },
        })
    );

    const client = new ApolloClient({
        link: wsLink,
        cache: new InMemoryCache(),
    });

    return client;
};

export default useGraphQL;