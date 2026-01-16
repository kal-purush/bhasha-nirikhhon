import { useQuery } from "@tanstack/react-query";
import axiosInstance from "../api/AxiosInstance";
import { AxiosError } from "axios";


interface SuccessResponse{
    dataList:ScheduleExercise[]
}

interface ScheduleExercise{
    schedule:Schedule
    exerciseList:Exercise[]
}

interface Schedule{
    memberId: number,
    scheduleId:number,
    scheduleType: string,
    scheduleDay1: string,
    scheduleDay2: string|null,
    scheduleDays: number,
    scheduleDescription: string,
    scheduleExpirayDate: string,
    scheduleRegisteredDate: string,
    scheduleValidTime: number,
    active: boolean
}

interface Exercise{
    exerciseName: string,
    reps: string,
    exerciseUrl: string,
    sets: number,
    duration: number
}

interface ErrorResponse{
    errorMessage:string
}

const useGetCurrentSchedules=(memberId:number)=>{
    console.log("member id",memberId);
    const getCurrentSchedule=async()=>{
        try{
        const {data}=await axiosInstance.get<SuccessResponse>(`/schedules/current/${memberId}`)
        return data.dataList;
        }catch(e){
            if(e instanceof AxiosError){
                const error=((e.response?.data) as ErrorResponse).errorMessage || "Request failed"
                throw new Error(error)
                        }
            console.log(e)
                throw new Error("Un expected error occured")
        }
    }

    return useQuery<ScheduleExercise[],Error>({
        queryKey:["currentScheduleList",memberId],
        queryFn:getCurrentSchedule
    })

}
export default useGetCurrentSchedules;