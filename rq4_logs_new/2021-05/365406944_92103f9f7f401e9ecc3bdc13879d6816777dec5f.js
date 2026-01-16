// Using the Express Module
const express = require('express')
const app = express()

//Importing Sequeleze
const { Sequelize, DataTypes } = require('sequelize') 
const TaskModel = require('./models/task')

// Connecting Sequelize with the database
const sequelize = new Sequelize ({
    dialect: 'sqlite',
    storage: 'taskslist.db'
})

// Declaring the connection and the data type
const tasks = TaskModel(sequelize, DataTypes)

// We need to parse JSON coming from requests
app.use(express.json())


// Welcome Page
    app.get('', (req, res) => {
    res.send('<html><body><h1>My Tasks List!</h1><p>Created by: Iramar Vasquez</p></body></html>')
  })

// Listing tasks by using GET
app.get('/tasks', async (req, res) => {
    const allTasks = await tasks.findAll()

    // Usando SQLite puro
    // const allTasks = await sequelize.query('SELECT * FROM Tasks')

    res.json({ action: 'Listing tasks', task: allTasks })
})
  
// Creating a task by using POST
app.post('/tasks', async(req, res) => {
    const body = req.body

    const newTask = await tasks.create({
        description: body.description,
        done: body.done
    })

    res.json({ action: 'New Task', task: newTask })
})

// Show a specific task using GET
app.get('/tasks/:id', async (req, res) => {
    
    const taskId = req.params.id
    const task = await tasks.findByPk(taskId)
    res.send({ action: 'Showing task', task: task })
})
  
  // Updating a task by using PUT
  app.put('/tasks/:id', async (req, res) => {
    const taskId = req.params.id
    const body = req.body
    const updateTask = await tasks.findByPk(taskId)
    updateTask.update({
        description: body.description,
        done: body.done  
    });

    res.send({ action: 'Updating task', task: updateTask })
  })
  
  // Delete task
  app.delete('/tasks/:id', async (req, res) => {
    const taskId = req.params.id
    const deletingTask = await tasks.destroy({ where: { ID: taskId } })
  
    res.send({ action: 'Deleting task', taskId: deletingTask})
  })
  
  app.listen(8080, () => {
    console.log('Starting ExpressJS in the 8080 port')
})