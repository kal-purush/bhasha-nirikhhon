import express from 'express';
import routes from './src/routes/crmRoutes';
import mongoose from 'mongoose';
import bodyParser from 'body-parser';

const app = express();
const PORT = 3000;

// mongoose connection
mongoose.Promise = global.Promise;
mongoose.connect('mongodb://localhost/test', {
    useMongoClient : true
});

// bodyParser Setup
app.use(bodyParser.urlencoded({ extended:true }));
app.use(bodyParser.json());

routes(app);


app.get('/' , (req,res)=>
    res.send(`Node And Express Server Is Running on port ${PORT}`)
);



app.listen(PORT,()=>
    console.log(` Server Is Running on port ${PORT}`)
);