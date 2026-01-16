import gql from 'graphql-tag'

export const LOGIN_MUTATION = gql`
  mutation($email: String!, $password: String!) {
    logIn(email: $email, password: $password) {
      id
      token
    }
}
`
export const SIGNUP_MUTATION = gql`
  mutation SIGNUP_MUTATION(
    $name: String!
    $email: String!
    $password: String!
  ) {
    signUp(name: $name, email: $email, password: $password) {
      id
      token
    }
  }
`

export const LOGOUT_MUTATION = gql`
  mutation {
    logOut {
      message
    }
  }
`