import { useMutation } from '@tanstack/react-query'
import {  AxiosError } from "axios"
import axiosInstance from '../api/AxiosInstance'



interface UserCredentials{
     email:string
     otp:string
}

type SuccessResponse=string;
type ErrorResponse=string;




const useOtpVerifcation = () => {
    return useMutation<SuccessResponse,AxiosError<ErrorResponse>,UserCredentials>({
        mutationFn:async({email,otp}:UserCredentials)=>{
            try{
                const {data}=await axiosInstance.post<SuccessResponse>('/auth/member/validate-otp',{
                        email,
                        otp
                })
                return data;
            }catch(e){
                if(e instanceof AxiosError){
                    console.log("error is in axios"+e.response?.data);
                    //in here it si checking errors according to error object
                    const error=e.response?e.response.data?e.response.data.error?e.response.data.error:e.response.data:"Connection error":"Unexpected error occured"
                    console.log("error is"+error);
                    throw new Error(error);
                }
                console.log("error is outside of if");
                throw new Error("unexpected error occured")
            }
        }
    })
}

export default useOtpVerifcation;