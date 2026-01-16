import { useMutation } from "@tanstack/react-query";
import { AxiosError } from "axios";
import axiosInstance from "../api/AxiosInstance";


interface ProfileImage{
    memberId:number,
    profileImage:FormData
}

type SuccessResponse=string;
type ErrorResponse=string;




const usePostProfileImage = () => {
   return useMutation<SuccessResponse,AxiosError<ErrorResponse>,ProfileImage>({
       mutationFn:async({memberId,profileImage}:ProfileImage)=>{
        console.log(memberId)
        console.log(JSON.stringify(profileImage))
           try{
               const {data}=await axiosInstance.post<SuccessResponse>(`/members/${memberId}/uploadFile`,profileImage,{
                headers: {
                    'Content-Type': 'multipart/form-data', // ✅ Allow FormData handling
                }
               })
               return data;
           }catch(e){
               if(e instanceof AxiosError){
                   console.log("error is in axios"+e.response?.data);
                   //in here it si checking errors according to error object
                   const error=e.response?e.response.data?e.response.data.error?e.response.data.error:e.response.data:"Connection error":"Unexpected error occured"
                   console.log("error is"+JSON.stringify(e.response));
                   throw new Error(error);
               }
               console.log("error is outside of if");
               throw new Error("unexpected error occured")
           }
       }
   })
}

export default usePostProfileImage;