import { Posts } from "../models/Post";

export const deletePost = async (postId: string) => {
    // move the post by moving it from posts to deletedPosts
    try {
        const res = await fetch(`/api/posts/${postId}`, {
            method: "GET"
        })

        if (!res) throw new Error("Internal Server Error")

        const resJson = await res.json()

        if (!res.ok) { throw new Error(resJson.data) }

        const post = resJson.data
        console.log(post)

        //const deleteRes = await fetch(`/api/posts/${postId}`, {
        //    method: "DELETE"
        //})

        //if (!deleteRes) throw new Error("Internal Server Error")
        //const deleteResJson = await deleteRes.json()
        //if (!deleteRes.ok) throw new Error(deleteResJson.data)

        //const { title, email, date, text, upvotes, downvotes, comments } = post

        //const newPost = {
        //    title,
        //    email,
        //    date,
        //    text,
        //    upvotes,
        //    downvotes,
        //    comments
        //}
        //const createRes = await fetch(`/api/posts/${postId}`, {
        //    method: "POST",
        //    body: JSON.stringify(newPost)
        //})
        //if (!createRes) throw new Error("Internal Server Error")
        //const createResJson = await createRes.json()

        //if (!createRes.ok) throw new Error(createResJson.data)
        //return { success: true, data: createResJson.data }
    }
    catch (e: Error | any) {
        console.error(e)
        return { success: false, error: e }
    }
}