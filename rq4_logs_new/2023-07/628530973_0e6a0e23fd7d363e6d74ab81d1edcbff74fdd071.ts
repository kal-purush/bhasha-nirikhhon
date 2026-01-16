import { useQuery } from '@tanstack/react-query'
import { currentUserQueryKey } from '../constants/QueryKeys'

// Fetch current user data from Supabase database
export default function useCurrentUser() {
  // const { user } = useAuth()

  const { data: currentUser, isFetching } = useQuery({
    queryKey: [currentUserQueryKey],
    queryFn: async () => {
      // console.log('useCurrentUser, user:', user)
      // if (!user) return null
      // const userId = user.id
      // return await getUserProfile(userId)
    },
    onSuccess: (data) => {
      console.log('fetched data', data)
    },
  })

  return {
    currentUser,
    isFetching,
  }
}