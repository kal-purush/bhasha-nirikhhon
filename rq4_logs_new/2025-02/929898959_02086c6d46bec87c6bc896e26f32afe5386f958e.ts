import { useMutation } from '@tanstack/react-query'
import {  AxiosError } from "axios"
import axiosInstance from '../api/AxiosInstance'



interface UserCredentials{
    userName:string,
    password:string
}

type SuccessResponse=string;
type ErrorResponse=string;




const usePasswordReset = () => {
    return useMutation<SuccessResponse,AxiosError<ErrorResponse>,UserCredentials>({
        mutationFn:async({userName,password}:UserCredentials)=>{
            try{
                const {data}=await axiosInstance.post<SuccessResponse>('/auth/member/reset-password',{
                        userName:userName,
                        password:password,
                })
                return data;
            }catch(e){
                if(e instanceof AxiosError){
                    const error=((e.response?.data.error) as ErrorResponse)||"Request failed"
                    console.log("error is inside of if")
                    console.log(error);
                    throw new Error(error);
                }
                console.log("error is outside of if");
                throw new Error("unexpected error occured")
            }
        }
    })
}

export default usePasswordReset;