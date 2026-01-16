
const express=require('express')
const mongoose=require('mongoose')
require('dotenv').config() //use env variables

const cors = require('cors');
const morgan = require('morgan')
const bodyParser = require('body-parser')
const cookieParser = require('cookie-parser')
const expressValidator = require('express-validator')


//import routes
const authRoutes=require('./routes/auth');
const userRoutes=require('./routes/user');
const categoryRoutes=require('./routes/category');
const productRoutes=require('./routes/product');

//app
const app=express()

//db

mongoose.Promise = global.Promise;
mongoose.connect(process.env.MONGO_URI,{
    useNewUrlParser:true,
    useCreateIndex: true,
    useUnifiedTopology: true
}).then(()=>{
    console.log('DB Connected');
}).catch(err=>console.log(err));

mongoose.connection.on('error',err=>{
    console.log(`DB connection error :${err}`)
})

// import mongooseconst mongoose = require('mongoose');
// load env variablesconst dotenv = require('dotenv');dotenv.config() 
//db connectionmongoose.connect(  process.env.MONGO_URI,  {useNewUrlParser: true}).then(() => console.log('DB Connected')) mongoose.connection.on('error', err => {  console.log(`DB connection error: ${err.message}`)});

//routes

app.use(morgan('dev'))
app.use(bodyParser.json())
app.use(cookieParser())
app.use(expressValidator())
app.use(cors());

//routes middleware
app.use("/api",authRoutes);
app.use("/api",userRoutes);
app.use("/api",categoryRoutes);
app.use("/api", productRoutes);



const port=process.env.PORT || 8000;




app.listen(port,()=>{
    console.log(`Server is running on ${port}`);
});