import { useMutation } from '@tanstack/react-query'
import axios, { Axios, AxiosError } from "axios"


interface UserCredentials{
    userName:string,
    password:string
}

interface SuccessResponse{
    successMessage:String,
    token:String
}

interface ErrorResponse{
    error:string
}


const useLogin = () => {
    return useMutation<SuccessResponse,AxiosError<ErrorResponse>,UserCredentials>({
        mutationFn:async({userName,password}:UserCredentials)=>{
            try{
                const {data}=await axios.post<SuccessResponse>('http://192.168.43.137:8080/api/v1/auth/token',{
                        userName:userName,
                        password:password,
                        userType:"MEMBER"
                })
                return data;
            }catch(e){
                if(e instanceof AxiosError){
                    const error=((e.response?.data) as ErrorResponse).error||"Request failed"
                    console.log(error);
                    throw e;
                }
                throw new Error("unexpected error occured")
            }
        }
    })
}

export default useLogin