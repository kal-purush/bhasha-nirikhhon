import axios from "axios"
import { Post } from "../Entities/postEntities"

export class postServices {
    private endpointpost: string
    private endpointuserpost: string
    private endpointspecificpost: string
    
    /*Falar com o Jonathan sobre isso -> */ private UserID: any
    /*Falar com o Jonathan sobre isso -> */ private postId: any

    constructor() {
        /*Falar com o Jonathan sobre isso -> */ this.UserID = ""
        /*Falar com o Jonathan sobre isso -> */ this.postId = ""
        
        this.endpointpost = "https://boopeepapir.onrender.com/posts"
        this.endpointuserpost = `https://boopeepapir.onrender.com/posts?UserID=${this.UserID}`,
        this.endpointspecificpost = `https://boopeepapir.onrender.com/posts?UserID=${this.postId}`
    };

    //------------------------------------------------------------------

    async getPosts(post: Post): Promise<{ valido: boolean, value?: number, error?: string | Error, data?: Post }> {
        const allPosts = {
            UserID: post.UserID,
            postId: post.postId,
            description: post.description,
            local: post.local,
            status: post.status,
            createdAt: post.createdAt,
        }

        try {
            console.log("getPosts foi chamado!");
            const resp = await axios.get(this.endpointpost, {
                headers: {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET",
                    "Content-Type": "application/json;charset=UTF-8"
                }
            })

            if (resp.status !== 201) {
                console.log('getPosts respondeu com ERRO!')
                throw new Error(resp.statusText)
            }
            console.log('getPosts respondeu com SUCESSO!')
            return { valido: true, value: 201, data: resp.data }

        } catch (error) {
            if (error instanceof Error) {
                return { valido: false, value: 400, error: error.message }
            }
            return { valido: false, value: 500, error: 'Internal Server Error' }
        }
    }

    //------------------------------------------------------------------

    async getPostFromUser(post: Post): Promise<{ valido: boolean, value?: number, error?: string | Error, data?: Post }> {
        try {
            console.log("getPostFromUser for chamado!")
            const resp = await axios.get(this.endpointuserpost, {
                headers: {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET",
                    "Content-Type": "application/json;charset=UTF-8"
                }
            })

            if (resp.status !== 201) {
                console.log('getPostFromUser respondeu com ERRO!')
                throw new Error(resp.statusText)
            }
            console.log('getPostFromUser respondeu com SUCESSO!')
            return { valido: true, value: 201, data: resp.data }

        } catch (error) {
            if (error instanceof Error) {
                return { valido: false, value: 400, error: error.message }
            }
            return { valido: false, value: 500, error: 'Internal Server Error' }
        }
    }

    //------------------------------------------------------------------

    async getSpecificPost(post: Post): Promise<{valido: boolean, value?: number, error?: string | Error, data?: Post}> {
        try {
            console.log("getSpecificPost foi chamado!")
            const resp = await axios.get(this.endpointspecificpost, {
                headers: {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET",
                    "Content-Type": "application/json;charset=UTF-8"
                }
            })

            if (resp.status !== 201) {
                console.log('getSpecificPost respondeu com ERRO!')
                throw new Error(resp.statusText)
            }
            console.log('getSpecificPost respondeu com SUCESSO!')
            return { valido: true, value: 201, data: resp.data }

        } catch (error) {
            if (error instanceof Error) {
                return { valido: false, value: 400, error: error.message }
            }
            return { valido: false, value: 500, error: 'Internal Server Error' }
        }
    }

    //------------------------------------------------------------------
};


// fetchPosts = async () => {
//     const response = await fetch('https://boopeepapir.onrender.com/posts');
//     if (!response.ok) {
//         throw new Error('Fetch response is not ok!');
//     }
//     return response.json();
// }

// //VERIFICAR A API, ENTÃO VER SE ELE SÓ PEGA OS POSTS DO USUÁRIO
// fetchPostsfromUser = async (UserID: string) => {
//     const response = await fetch(`https://boopeepapir.onrender.com/posts/${UserID}`)
//     if (!response.ok) {
//         throw new Error('Fetch response is not ok!');
//     }
//     return response.json();
// }

// fetchPostsfromPost = async (postId: string) => {
//     const response = await fetch(`https://boopeepapir.onrender.com/posts/${postId}`)
//     if ("response.ok") {
//         throw new Error('Fetch response is not ok!')
//     }
//     return response.json();
// }