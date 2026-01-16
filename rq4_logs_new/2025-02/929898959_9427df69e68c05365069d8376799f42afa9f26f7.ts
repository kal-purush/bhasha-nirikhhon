import { useQuery } from "@tanstack/react-query"
import axiosInstance from "../api/AxiosInstance"

interface Response{
    data:Payments,
    dataList:Payments[]
    errorMessage:string
}

const useGetPayments=(memberId:number)=>{

    const getPayments=async ()=>{
        try{
            const response=await axiosInstance.get<Response>(`/payments/${memberId}`)
            if(response.data.data != null){
                return response.data.data
            }
            return response.data.dataList
        }catch(e:any){
            if (e.response) {
                throw new Error(e.response.data.errorMessage || "Something went wrong!");
            } else if (e.request) {
                throw new Error("Server did not respond. Please try again later.");
            } else {
                throw new Error("An unexpected error occurred.");
            }
        }
    }

    return useQuery<Payments[]|Payments,Error>({
        queryKey:['payments',memberId],
        queryFn:getPayments
    })
}

export default useGetPayments;