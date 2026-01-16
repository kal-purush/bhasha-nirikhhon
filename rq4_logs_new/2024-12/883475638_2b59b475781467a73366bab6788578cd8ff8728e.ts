import { Request, Response, NextFunction } from 'express';
import { pool } from '../database/CarmineDB'

export const validateGetTask = async (req: Request, res: Response, next: NextFunction) => {
   try {
        const result = await pool.query('SELECT * FROM tasks');
        
        if (result.rows.length === 0) {
            console.log("No tasks found in the database");
            return res.status(404).json({ message: "No tasks found" });
        }

        console.log("Tasks found in the database");
        next(); 
    } catch (error) {
        console.error('Error checking for tasks:', error);
        res.status(500).json({ message: 'Internal server error' });
    }
}

export const validateInsertTask = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { task_id, text, createdAt, dueAt, completed } = req.body;

        if (!task_id || !text || !createdAt || !dueAt || typeof completed !== 'boolean') {
            console.log("Task data could not be inserted");
            return res.status(400).json({ message: 'All fields must be provided and valid' });
        }
        console.log("Task data inserted successfully");
        next(); 
    } catch (error) {
        console.error('Error inserting tasks:', error);
        res.status(500).json({ message: 'Internal server error' });
    }
}

export const validateDeleteTask = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { task_id } = req.params;

        if (!task_id) {
            console.log("Task data not deleted");
            return res.status(400).json({ message: 'Task ID is not found' });
        }

        console.log("Task deleted succesfully")
        next();
    } catch (error) {
        console.error('Error validating task deletion:', error);
        res.status(500).json({ message: 'Internal server error' });
    }
}

export const validateCompleteTask = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { task_id } = req.params;
        const { completed } = req.body;
    
        if (!task_id) {
            console.log("Task could not find id for completing task");
            return res.status(400).json({ message: 'Task ID is required' });
        }
    
        if (typeof completed !== 'boolean') {
            console.log("Task could not check/uncheck");
            return res.status(400).json({ message: 'Completed must be a boolean' });
        }
        
        console.log("Task successfully check/uncheck")
        next();
    } catch (error) {
        console.error('Error validating task completion:', error);
        res.status(500).json({ message: 'Internal server error' });
    }
}

export const validateUpdateTask = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { task_id } = req.params;
        const { text, dueAt } = req.body;
    
        if (!task_id) {
            console.log("Task could not find id for updating task");
            return res.status(400).json({ message: 'Task ID is required' });
        }
    
        if (text !== undefined && typeof text !== 'string') {
            console.log("Invalid String for updating");
            return res.status(400).json({ message: 'Text must be a string' });
        }
    
        if (dueAt !== undefined) {
            const dueDate = new Date(dueAt);
            if (isNaN(dueDate.getTime())) {
                console.log("Invalid Date for updating");
                return res.status(400).json({ message: 'Due date must be a valid date' });
            }
        }

        console.log("Task successfully updated")
        next();
    } catch (error) {
        console.error('Error validating update task:', error);
        res.status(500).json({ message: 'Internal server error' });
    }
}