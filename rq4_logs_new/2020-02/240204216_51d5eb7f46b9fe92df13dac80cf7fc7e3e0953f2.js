import express from 'express';
import { createTodo, retrieveTodos, updateTodo } from '../controllers/todoController'

const router = express.Router();

// Create a new todo with value provided by user
router.post("/create", (req, res) => createTodo(req, res));

// Retrieve all todos
router.get("/retrieve", (req, res) => retrieveTodos(req, res));

// Update completed status of todo with ID provided by user
router.put("/update", (req, res) => updateTodo(req, res));

export default router;