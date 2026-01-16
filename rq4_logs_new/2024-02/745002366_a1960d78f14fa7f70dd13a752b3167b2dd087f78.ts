import { userStore } from '@/store/authStore.ts'
import { useEffect, useState } from 'react'
import { supabase } from '@/config/supabase/supabaseClient.ts'
import { userDetails } from '@/model/user.ts'

export const useUser = (): userDetails => {
  const { session } = userStore.useStore()
  const [user, setUser] = useState<userDetails>()

  useEffect(() => {
    async function fetchPosts() {
      const { data } = await supabase
        .from('USER')
        .select('*')
        .eq('id', String(session?.user.id))

      return data
    }

    fetchPosts().then((data) => {
        const user: userDetails = {
            id: data?.at(0)?.id || '',
            USERNAME: data?.at(0)?.USERNAME || '',
            FIRST_NAME: data?.at(0)?.FIRST_NAME || '',
            LAST_NAME: data?.at(0)?.LAST_NAME || '',
        }

        setUser(user)
    })
  }, [session?.user.id])

    return user === undefined ? {
        id: '',
        USERNAME: '',
        FIRST_NAME: '',
        LAST_NAME: ''
  } : user
}