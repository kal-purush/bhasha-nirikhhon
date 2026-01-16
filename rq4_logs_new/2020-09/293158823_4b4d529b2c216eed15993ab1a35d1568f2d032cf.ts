import { APOLLO_OPTIONS } from 'apollo-angular';
import { HttpLink } from 'apollo-angular/http';
import { split, ApolloClientOptions, DefaultOptions, from } from '@apollo/client/core';
import { WebSocketLink } from '@apollo/client/link/ws';
import { getMainDefinition } from '@apollo/client/utilities';
import { AngularFireAuth } from '@angular/fire/auth';
import { setContext } from '@apollo/client/link/context';

import { take } from 'rxjs/operators';
import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';
import { OperationDefinitionNode } from 'graphql';
import { InMemoryCache, ApolloLink } from '@apollo/client/core';
const uri = 'http://localhost:8080/v1/graphql';
const wsUri = 'ws://localhost:8080/v1/graphql';

export function provideApollo(httpLink: HttpLink, auth: AngularFireAuth) {
  const basic = setContext((operation, context) => ({
    headers: {
      Accept: 'charset=utf-8',
    },
  }));

  // Get the authentication token from local storage if it exists

  const authenticated = setContext(async (operation, context) => {
    const token = await auth.idToken.pipe(take(1)).toPromise();
    console.log('Create context');
    return {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    };
  });

  const http = httpLink.create({
    uri,
  });

  const ws = new WebSocketLink({
    uri: wsUri,

    options: {
      reconnect: true,
      lazy: true,
      connectionParams: async () => {
        const token = await auth.idToken.pipe(take(1)).toPromise();
        return {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        };
      },
    },
  });

  // using the ability to split links, you can send data to each link
  // depending on what kind of operation is being sent
  const link2 = ApolloLink.split(
    // split based on operation type
    ({ query }) => {
      const { kind, operation } = getMainDefinition(query) as OperationDefinitionNode;
      return kind === 'OperationDefinition' && operation === 'subscription';
    },
    ws,
    http,
  );

  const link = ApolloLink.from([basic, authenticated, link2]);

  const defaultOptions: DefaultOptions = {
    watchQuery: {
      fetchPolicy: 'no-cache',
      errorPolicy: 'ignore',
    },
    query: {
      fetchPolicy: 'no-cache',
      errorPolicy: 'all',
    },
  };

  const cache = new InMemoryCache();

  return {
    link,
    cache,
    defaultOptions,
  };
}

@NgModule({
  exports: [HttpClientModule],
  providers: [
    {
      provide: APOLLO_OPTIONS,
      useFactory: provideApollo,
      deps: [HttpLink, AngularFireAuth],
    },
  ],
})
export class GraphQLModule {}