const express = require('express');
const app =express() ;
const port = process.env.PORT || 5000 ;
const connectDB =require('./config/db');

//calling db
connectDB();
 
//init middleware
app.use(express.json({extended : false}))

app.get('/',(req,res) => {
     res.send('response send successfully');
})

app.use('/api/addTask', require('./routes/addTasks'));
app.use('/api/getTasksList', require('./routes/fetchAllTasks'));
app.use('/api/getTask', require('./routes/fetchTaskBasedOnUser'));
app.use('/api/inviteTeammate', require('./routes/inviteTeammate'));
app.use('/api/updateTaskStatus', require('./routes/updateTaskStatus'));
app.use('/api/login',require('./routes/login'));
app.use('/api/register', require('./routes/registration'));

app.listen(port , () => console.log(`app is lisning on port ${port}`))