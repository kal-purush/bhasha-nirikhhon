import Meal from "./meal";
import Exercise from "./exercise";

export default interface Day{
    completedGoal:boolean,
    caloriesBurnt:number,
    Meal:CheckMeal[]
    Exercise:CheckExercise[]

}

interface CheckExercise extends Exercise{
    completed:boolean
}
interface CheckMeal extends Meal{
    completed:boolean
}