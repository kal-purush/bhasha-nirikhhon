require('dotenv').config()
const express = require('express')
const mongoose = require('mongoose')
const authRoute = require("./routes/auth")
const employeeRoute = require("./routes/employee")
const companyRoute = require("./routes/company")

const app = express()

app.use(express.json());


app.use('/api/auth', authRoute)
app.use('/api/employee', employeeRoute)
app.use('/api/company', companyRoute)



mongoose.connect(process.env.DATABASE_URI).then(()=>{
    console.log('DB connected successfully')
    app.listen(process.env.PORT || 3500, () => {
    console.log('Server is running')
})
}).catch((err) => console.log(err.message))