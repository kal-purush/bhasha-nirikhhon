import { insertWorkout } from "../mutations/Workout";
import { insertProgramWorkout } from "../mutations/ProgramWorkout";
import { insertWorkoutExercise } from "../mutations/WorkoutExercise";
import { insertWorkoutExerciseSet } from "../mutations/WorkoutExerciseSet";
import { IWorkoutData } from "../data/workouts";
import { IProgramWorkoutData } from "../data/programs";

// TODO: Error catch and roll back changes if at all possible?
export async function addWorkout(workout: IWorkoutData, programId?: number) {
  if (!workout?.exercises?.length) {
    throw new Error("Workouts must contain at least one exercise");
  }
  const workoutId = await insertWorkout(workout);

  if (programId) {
    await insertProgramWorkout(
      programId,
      workoutId, // TODO: Ensure this is required in creating a program workout!
      (workout as IProgramWorkoutData).week,
      (workout as IProgramWorkoutData).day
    );
  }

  if (workout?.exercises?.length) {
    for await (const [
      exerciseIndex,
      workoutExercise,
    ] of workout.exercises.entries()) {
      const workoutExerciseId = await insertWorkoutExercise(
        workoutId,
        workoutExercise.id,
        exerciseIndex
      );

      // TODO: FUTURE FEATURE = Validate that the set matches the exercise (eg. reps, weight vs time, distance)
      if (workoutExercise?.sets?.length) {
        for await (const [
          setIndex,
          workoutExerciseSet,
        ] of workoutExercise.sets.entries()) {
          await insertWorkoutExerciseSet(
            workoutExerciseId,
            setIndex,
            workoutExerciseSet.reps,
            workoutExerciseSet.weight,
            workoutExerciseSet.time,
            workoutExerciseSet.distance
          );
        }
      }
    }
  }

  return workoutId;
}