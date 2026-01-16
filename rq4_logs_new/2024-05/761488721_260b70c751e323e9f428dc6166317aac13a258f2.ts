import { useState } from "react"
import { postValidator } from "../../Service/Validators/postValidator"
import { postServices } from "../../Service/API/postService"
import { Post } from "../../Service/Entities/postEntities"
import { postStateAndSetters } from "../../utils/types/Interfaces/postStateAndSetters"

const feedStateController = () => {
    //const [title, setTitle] = useState("")
    const [description, setDescription] = useState("")
    const [UserID, setUserID] = useState("")
    const [postId, setPostId] = useState("")
    const [local, setLocal] = useState("")
    const [status, setStatus] = useState(0)
    const [createdAt, setCreatedAt] = useState(0)

    // const validator: postValidator = new postValidator()
    const postService: postServices = new postServices()

    const setState: postStateAndSetters = {
        //title: setTitle,
        description: setDescription,
        local: setLocal,
        status: setStatus,
        createdAt: setCreatedAt
    }

    // const handleFeedFetch = async (/*title: string,*/ description: string, local: string, status: number, createdAt: number): Promise<{valido: boolean, value?: number, erro?: string | Error, data?: Post}> {
        
    // }

    const handleFeedFetch = async (createdAt: number, UserID: string, postId: string, local: string, status: number, description: string): Promise<{valido: boolean, value?: number, erro?: string | Error, data?: Post}> => {
        const post: Post = new Post (
            createdAt,
            UserID,
            postId,
            local,
            status,
            description        
        );   
        
        try {
            console.log(post)
            const req = await postService.getPosts(post)
            if (req.valido === false) {
                throw new Error("Bad Request");
            }
            return { valido: true, value: 201, data: post };
        } catch (error) {
            if (error instanceof Error) {
                if (error.message === "Unauthorized") {
                  return { valido: false, value: 401, erro: error };
                } else if (error.message === "Bad Request") {
                  return { valido: false, value: 400, erro: error };
                }
            }
              return { valido: false, value: 500, erro: "Internal Server Error" };
        }
    }

    return {description, local, status, createdAt, UserID, postId, handleFeedFetch};
};

export { feedStateController }