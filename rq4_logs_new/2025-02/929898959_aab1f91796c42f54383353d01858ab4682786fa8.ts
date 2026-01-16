import { useMutation } from '@tanstack/react-query'
import {  AxiosError } from "axios"
import axiosInstance from '../api/AxiosInstance'



interface UserCredentials{
    userName:string,
     email:string
}

type SuccessResponse=string;
type ErrorResponse=string;




const useEmailVerifcation = () => {
    return useMutation<SuccessResponse,AxiosError<ErrorResponse>,UserCredentials>({
        mutationFn:async({userName,email}:UserCredentials)=>{
            try{
                const {data}=await axiosInstance.post<SuccessResponse>('/auth/member/forgot-password',{
                        userName,
                        email
                })
                return data;
            }catch(e){
                if(e instanceof AxiosError){
                    const error=e.response?e.response.data.error?e.response.data.error:e.response.data:"Unexpected error occured"
                    console.log("error is"+error);
                    throw new Error(error);
                }
                console.log("error is outside of if");
                throw new Error("unexpected error occured")
            }
        }
    })
}

export default useEmailVerifcation;