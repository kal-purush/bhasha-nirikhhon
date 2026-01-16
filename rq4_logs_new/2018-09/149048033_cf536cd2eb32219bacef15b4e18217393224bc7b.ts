import { Apollo, gql } from 'apollo-angular-boost';
export default {

  defaults: {
    users: []
  },
  resolvers: {
    Query: {
      users: (_, params, { cache }) => {
        let usersFromCache = cache.readQuery({ query: GET_USERS_QUERY })
        return usersFromCache.users
      },
      user: (_, params, { cache }) => {
        let userId = `User:${params.id}`;
        const fragment = gql`
            fragment user on User {
              id
              name
            }`
        return cache.readFragment({ fragment, id: userId })
      }
    },
    Mutation: {
      addUser: (_, params, { cache }) => {
        const previous = cache.readQuery({ query: GET_USERS_QUERY });
        const newUser = {
          id: params.id,
          name: params.name,
          __typename: 'User'
        };

        const data = {
          users: [...previous.users.concat([newUser])],
        };

        cache.writeData({ data });
        return newUser
      }
    }
  }
}
//
// QUERIES
//
export const ADD_USER_QUERY = gql`
  mutation addUser($id: ID!, $name:string) {
    addUser (id: $id, name : $name)  @client {
      id
      name
    }
  }
`;


export const GET_USERS_QUERY = gql`
  query getUsers {
    users  @client {
      id
      name
    }
  }
  `;
export const GET_USER_QUERY = gql`
  query user($id: ID!) {
    user (id: $id) @client {
      id
      name
    }
  }
`;