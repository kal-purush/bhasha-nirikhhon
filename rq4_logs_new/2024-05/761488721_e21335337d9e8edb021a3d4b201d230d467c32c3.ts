import { useQuery } from "@tanstack/react-query"
import postServices from "../../Service/API/postAPIService"

const commentPostController = ({postId}: any) => {
    const {data, isError, isLoading, error} = useQuery({
        queryKey: ['postcommentdata'],
        queryFn: async () => {
            const commentPostQueryFunction = new postServices();
            return await commentPostQueryFunction.fetchPostsfromPost(postId)
        }
    })

    return {data, isError, isLoading, error}
}

export default commentPostController