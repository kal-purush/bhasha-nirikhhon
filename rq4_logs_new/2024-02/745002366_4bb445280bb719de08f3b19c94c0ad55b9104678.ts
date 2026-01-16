import { supabase } from "@/config/supabase/supabaseClient"
import { userStore } from "@/store/authStore"
import { useEffect, useState } from "react"

export const useFollowingUsers = () => {
    const { session } = userStore.useStore()
    const [following, setFollowing] = useState<string[]>([])

    const userId = session?.user?.id || ''

    useEffect(() => {
        async function fetchFollowing() {
            const data = await supabase.from("FOLLOWER").select("FOLLOWING").eq("follower", userId)

            return data
        }

        fetchFollowing().then((data) => {
            const fetchedFollowing: string[] = data?.data?.map((elem) => elem.FOLLOWING) || []
            
            setFollowing([...fetchedFollowing, userId])
        })
    })
    
    const startFollow = (followingId: string) => {
        supabase.from("FOLLOWER").insert({ follower: userId, FOLLOWING: followingId }).then(() => {
            setFollowing([...following, followingId])
        }, (err) => {
            console.log(err)    // add a toast error
        })
    }

    const unFollow = (unfollowingId: string) => {
        supabase.from("FOLLOWER").delete().eq("follower", userId).eq("FOLLOWING", unfollowingId).then(() => {
            setFollowing(following.filter((elem) => elem !== unfollowingId))
        }, (err) => {
            console.log(err)    // add a toast error
        })
    }

    return {following, startFollow, unFollow}
}