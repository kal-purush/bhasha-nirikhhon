import { userStore } from '@/store/authStore.ts'
import { useEffect, useState } from 'react'
import { supabase } from '@/config/supabase/supabaseClient.ts'
import { PostDetail } from '@/model/post.ts'

export const useHomePagePosts = (): PostDetail[] => {
  const { session } = userStore.useStore()
  const [posts, setPosts] = useState<PostDetail[]>([])

  useEffect(() => {
    async function fetchPosts() {
      const { data } = await supabase
        .from('POST')
        .select('*, USER!POST_author_fkey ( * )')
        .eq('author', String(session?.user.id))

      return data
    }

    fetchPosts().then((data) => {
      const mappedPosts: PostDetail[] =
        data?.map((post) => ({
          id: post.id,
          CONTENT: post.CONTENT,
          PUBLISHED_AT: post.PUBLISHED_AT,
          USERNAME: post.USER?.USERNAME || '',
          FIRST_NAME: post.USER?.FIRST_NAME || '',
          LAST_NAME: post.USER?.LAST_NAME || '',
        })) || []

      setPosts(mappedPosts || [])
    })
  }, [])

  return posts
}