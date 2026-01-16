import { create } from "zustand";

interface Exercise{
    exerciseId:number,
    exerciseStatus:boolean,
    totalSets: number,
    duration:number,
    completedSets:number
}

interface ScheduleState{
    scheduleExerciseList:Exercise[],
    setInitialScheduleExercise:(exercises:Exercise[])=>void,
    updateScheduleExercise:(exerciseId:number)=>void
}

export const useScheduleExerciseStore=create<ScheduleState>((set)=>({
    scheduleExerciseList:[],
    setInitialScheduleExercise:(exercises)=>{
        set(()=>{
            if(exercises.length>0){
                const initializedExerciseList:Exercise[]=exercises.map((exercise,index)=>({
                    exerciseId:exercise.exerciseId,
                    totalSets:exercise.totalSets??0,
                    duration:exercise.duration??0,
                    completedSets:0,
                    exerciseStatus:index==0
                }))
                return{scheduleExerciseList:initializedExerciseList}
            }
            return {scheduleExerciseList:[]}
        })
    },
    updateScheduleExercise:(exerciseId,remainingTime=0)=>{
        set((store)=>{
            const selectedExercise=store.scheduleExerciseList.find((exercise)=>exercise.exerciseId===exerciseId)

            if(!selectedExercise){
                return store
            }

            if(selectedExercise?.totalSets>0){
                if(selectedExercise.completedSets<selectedExercise.totalSets){
                    const completeSets=selectedExercise.completedSets+1;
                    return {
                        scheduleExerciseList:store.scheduleExerciseList.map((exercise)=>(
                            exercise.exerciseId===exerciseId?
                            {...exercise,completeSets:completeSets}:exercise
                        ))
                    }
                }
                return {
                    scheduleExerciseList:store.scheduleExerciseList.map((exercise)=>(
                        exercise.exerciseId===exerciseId?
                        {...exercise,exerciseStatus:true}:exercise
                    ))
                }
            }else{
                if(remainingTime != 0){
                    return {
                        scheduleExerciseList:store.scheduleExerciseList.map((exercise)=>(
                            exercise.exerciseId===exerciseId?
                            {...exercise,duration:remainingTime}:exercise
                        ))
                    }
                }
                return {
                    scheduleExerciseList:store.scheduleExerciseList.map((exercise)=>(
                        exercise.exerciseId===exerciseId?
                        {...exercise,exerciseStatus:true}:exercise
                    ))
                }

            }
        })
    }
}))