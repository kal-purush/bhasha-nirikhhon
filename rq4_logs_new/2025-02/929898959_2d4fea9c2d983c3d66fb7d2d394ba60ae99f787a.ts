export interface ScheduleExercise{
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