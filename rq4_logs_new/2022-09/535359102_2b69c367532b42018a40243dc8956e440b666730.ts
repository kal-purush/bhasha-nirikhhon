import express from "express";
import db from './models/database.config'
import cors from 'cors'

db.sync().then(() => {
    console.log('connect to db');

})

const app =express()



app.use(express.json())



 const port = 3000





app.listen(port, ()=> {
    console.log(`app listening on ${port}`);
    
})

export default app