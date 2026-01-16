import { supabase } from "@/config/supabase/supabaseClient"
import { useEffect, useState } from "react"
import { PostDetail } from '@/model/post.ts'

export const useDetailPosts = (postId: string): PostDetail | undefined => {
    const [post, setPost] = useState<PostDetail>()

    useEffect(() => {
        async function fetchPost(id: string) {
            const data = await supabase.
                from('POST')
                .select('*, USER!POST_author_fkey ( * )')
                .eq('id', id)

            return data
        }

        fetchPost(postId).then((data) => {
            // add some checks for undefined somewhere
            const postDetail: PostDetail = {
                id: data?.data?.at(0)?.id || '',
                CONTENT: data?.data?.at(0)?.CONTENT || '',
                PUBLISHED_AT: data?.data?.at(0)?.PUBLISHED_AT || '',
                USERNAME: data?.data?.at(0)?.USER?.USERNAME || '',
                FIRST_NAME: data?.data?.at(0)?.USER?.FIRST_NAME || '',
                LAST_NAME: data?.data?.at(0)?.USER?.LAST_NAME || '',
            }

            setPost(postDetail || undefined)
        })
    })

    return post
}