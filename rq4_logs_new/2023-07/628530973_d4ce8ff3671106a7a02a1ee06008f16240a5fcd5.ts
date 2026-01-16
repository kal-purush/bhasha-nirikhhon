import { SignInWithOAuthCredentials } from '@supabase/supabase-js'
import { QueryClient, useMutation, useQuery } from '@tanstack/react-query'
import _ from 'lodash'
import { useNavigate } from 'react-router-dom'
import { baseUrl } from '../constants/Environment'
import { currentAuthQueryKey } from '../constants/QueryKeys'
import { supabase } from './SupabaseProvider'

type AuthMutationPayload = ({ type: 'signInWithOAuth' } & SignInWithOAuthCredentials) | { type: 'signOut' }

export default function useAuth() {
  const { data: sessionData, isFetching } = useQuery({
    queryKey: [currentAuthQueryKey],
    queryFn: async () => {
      const { data: sessionData, error } = await supabase.auth.getSession()
      if (error) {
        throw error
      }
      console.log('session Data session', sessionData.session?.user)
      return sessionData
    },
    onError: (error: Error) => {
      alert(error.message)
    },
  })

  const user = sessionData?.session?.user
  const isLoggedIn = !!user

  const queryClient = new QueryClient()
  const navigate = useNavigate()

  const { mutateAsync } = useMutation({
    mutationFn: async (payload: AuthMutationPayload) => {
      switch (payload.type) {
        case 'signInWithOAuth':
          return supabase.auth.signInWithOAuth(_.omit(payload, 'type'))

        case 'signOut':
          return supabase.auth.signOut()
      }
    },
    onMutate: async (payload: AuthMutationPayload) => {
      switch (payload.type) {
        case 'signOut':
          await queryClient.setQueryData([currentAuthQueryKey], () => null)
          break
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries([currentAuthQueryKey])
      navigate('/login')
    },
  })

  const signIn = async () => {
    await mutateAsync({
      type: 'signInWithOAuth',
      provider: 'google',
      options: {
        redirectTo: baseUrl + 'login',
      },
    })
  }

  const signOut = async () => {
    await mutateAsync({ type: 'signOut' })
  }

  return { sessionData, user, isFetching, isLoggedIn, signIn, signOut }
}