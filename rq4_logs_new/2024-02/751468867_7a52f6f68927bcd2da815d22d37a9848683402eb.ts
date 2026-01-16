import { Request, Response } from 'express';
import { WorkoutService } from '../services/WorkoutService';
import { Workout } from '../database/entities/Workout';

const workoutService = new WorkoutService();

export const createWorkout = async (req: Request, res: Response) => {
    console.log('createWorkout');
    console.log(req.body);
    try {
        const workout = await workoutService.createWorkout(req.body);
        res.status(201).json(workout);
    } catch (error) {
        res.status(500).json({ error: 'Failed to create workout' });
    }
}

export const getAllWorkouts = async (req: Request, res: Response) => {
    try {
        const workouts = await workoutService.getAllWorkouts();
        res.json(workouts);
    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch workouts' });
    }
}

export const getWorkoutById = async (req: Request, res: Response) => {
    const workoutId = req.params.id;
    try {
        const workout = await workoutService.getWorkoutById(workoutId);
        if (workout) {
            res.json(workout);
        } else {
            res.status(404).json({ error: 'Workout not found' });
        }
    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch workout' });
    }
}

export const updateWorkout = async (req: Request, res: Response) => {
    const workoutId = req.params.id;
    try {
        const workout = await workoutService.updateWorkout(workoutId, req.body);
        if (workout) {
            res.json(workout);
        } else {
            res.status(404).json({ error: 'Workout not found' });
        }
    } catch (error) {
        res.status(500).json({ error: 'Failed to update workout' });
    }
}

export const deleteWorkout = async (req: Request, res: Response) => {
    const workoutId = req.params.id;
    try {
        const result = await workoutService.deleteWorkout(workoutId);
        if (result) {
            res.json({ message: 'Workout deleted successfully' });
        } else {
            res.status(404).json({ error: 'Workout not found' });
        }
    } catch (error) {
        res.status(500).json({ error: 'Failed to delete workout' });
    }
}
