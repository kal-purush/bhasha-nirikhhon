require('dotenv').config();
const mongoose = require('mongoose');

mongoose.connect(process.env.DB,{
    useCreateIndex:true,
    useFindAndModify:false,
    useNewUrlParser:true,
    useUnifiedTopology:true
}).then(()=>{
    console.log('server connected');
}).catch((e)=>{
    console.log("server not running");
})