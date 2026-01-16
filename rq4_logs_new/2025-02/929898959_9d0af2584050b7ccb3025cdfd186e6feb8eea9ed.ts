import { AxiosError } from "axios";
import axiosInstance from "../api/AxiosInstance";
import { Exercise } from "../interfaces/Exercise";
import { useQuery } from "@tanstack/react-query";


interface SuccessResponse{
    data:Exercise
}


interface ErrorResponse{
    errorMessage:string
}

const useGetExerciseById=(exerciseName:string)=>{
    const getCurrentSchedule=async()=>{
        try{
        const {data}=await axiosInstance.get<SuccessResponse>(`/exercises/${exerciseName}`)
        console.log(data.data)
        return data.data;
        }catch(e){
            console.log(e)
            if(e instanceof AxiosError){
                const error=((e.response?.data) as ErrorResponse).errorMessage || "Request failed"
                throw new Error(error)
                        }
            
                throw new Error("Un expected error occured")
        }
    }

    return useQuery<Exercise,Error>({
        queryKey:["currentScheduleList",exerciseName],
        queryFn:getCurrentSchedule
    })

}
export default useGetExerciseById;