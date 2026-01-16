import { ApolloError, gql, useMutation } from "@apollo/client"
import { doNothing } from "../../util"

interface AuthenticateResponse {
  authenticate: {
    accessToken: string
  }
}

interface Credentials {
  username: string
  password: string
}

interface AuthenticateVariables {
  credentials: Credentials
}

const AUTHENTICATE = gql`
  mutation (
    $credentials: AuthenticateInput
  ) {
    authenticate(
      credentials: $credentials
    ) {
      accessToken
    }
  }
`

export const useAuthenticate = () => {
  const [ mutate, result ] = useMutation<AuthenticateResponse, AuthenticateVariables>(AUTHENTICATE)

  const authenticate = (credentials: Credentials, onError?: (error: ApolloError) => void) => {
    mutate({
      variables: { credentials },
      onCompleted: data => {
        console.log("sign in OK, received token =", data.authenticate.accessToken)
      },
      onError: onError || doNothing
    })
  }

  return { authenticate, result }
}