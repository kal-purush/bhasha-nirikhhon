import { useApolloClient } from "@apollo/client"
import { useNavigate } from "react-router-native"
import { useAuthStorage, useNotifier } from "../contexts"
import { useAuthenticateMutation } from "../graphql"
import { Credentials } from "../types"

export const useSignIn = () => {
  const apolloClient = useApolloClient()
  const navigate = useNavigate()
  const [ mutate ] = useAuthenticateMutation()
  const { setAccessToken } = useAuthStorage()
  const { notifyError, notifySuccess } = useNotifier()

  const signIn = (credentials: Credentials) => {
    mutate({
      variables: {
        credentials
      },
      onCompleted: async data => {
        await setAccessToken(data.authenticate.accessToken)
        apolloClient.resetStore()
        notifySuccess("You are now signed in")
        navigate("/", { replace: true })
      },
      onError: error => {
        console.error(error)
        notifyError("Invalid username or password")
      }
    })
  }

  return { signIn }
}