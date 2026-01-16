const express = require('express');
const mongoose = require('mongoose');
const bodyParser = require('body-parser');
const cors = require('cors');

const app = express();
const port = process.env.PORT || 5000;
app.use(cors());
// Body parser middleware
app.use(bodyParser.json());

// MongoDB URI

const db = 'mongodb+srv://shivanshvikramsingh764:<password>@cluster0.65gkk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';

// Connect to MongoDB
mongoose
  .connect(db, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('MongoDB Connected'))
  .catch(err => console.log(err));

// Models
require('./task');
const Task = mongoose.model('tasks');

// Routes
app.get('/tasks', async (req, res) => {
    try {
        const tasks = await Task.find();
        res.json(tasks);
    } catch (error) {
        res.status(500).send('Server error');
    }
});

app.get('/tasks/:id', async (req, res) => {
    const task = await Task.findById(req.params.id);
    if (!task) return res.status(404).send('Task not found');
    res.json(task);
});

app.post('/tasks', async (req, res) => {
    try {
        const newTask = new Task(req.body);
        await newTask.save();
        res.status(201).send(newTask);
    } catch (error) {
        // Handle validation errors
        if (error.name === 'ValidationError') {
            return res.status(400).json({
                message: 'Validation error',
                errors: error.errors
            });
        }
        res.status(500).send('Server error');
    }
});

app.put('/tasks/:id', async (req, res) => {
    const updatedTask = await Task.findByIdAndUpdate(req.params.id, req.body, { new: true });
    res.json(updatedTask);
});

app.delete('/tasks/:id', async (req, res) => {
    await Task.findOneAndDelete(req.params.id);
    res.send({ message: 'Task deleted' });
});

app.listen(port, () => console.log(`Server running on port ${port}`));