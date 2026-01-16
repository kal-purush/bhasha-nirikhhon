const express = require('express')
const app = express()
const mysql = require('mysql')
const cors = require('cors')

//middlewares
app.use(cors())
app.use(express.json())

const db = mysql.createConnection({
    host:'localhost',
    user:'root',
    password:'aridb',
    database:'management',

})



// app.get('/',(req,res)=>{
//     console.log("It's is working no worries......");
//     return res.send(200).json({err :"No worries...."})
// })

app.post('/newemp',(req,res)=>{
    
    try{
        const {emp_id,name,email,phone,dept,d_join,role} = req.body;
        
        const qr = "INSERT INTO employee (emp_id,name,email,phone,dept,d_join,role) VALUES(?,?,?,?,?,?,?);"
        const values = [emp_id,name,email,phone,dept,d_join,role];
    
        db.query(qr,values,(err,result)=>{
            if(err){
                console.error("DB insert error...",err);
                return res.status(500).json({error: "Insert failed!"})
            }
            console.log(res);
            
            return res.status(201).json("Register Successfully!");
        })
    }catch(err){
        console.log(err);
    }
})

app.delete('/:id',async (req,res)=>{
    try{    

        const empid = req.params.id;
        const qr = "DELETE FROM employee where emp_id = ?;"  
        db.query(qr,empid,(err,result)=>{
            if(err){
                return res.status(500).json({"Can't able to delete!":err});
            }
            console.log(result);
            return res.status(200).json({"Deleted successfully":result});
        })  

    }catch(err){
        console.error(err);
        
    }
  
})

app.get('/',(req,res)=>{
    try{
        const qr = "SELECT * from employee";

        db.query(qr,(err,result)=>{
            if(err){
                console.log("Error while getting data");
                return res.status(500).json({"Error while getting":err});
            }
            return res.status(200).json(result);
        })
    }catch(err){
        console.log(err);
        
    }
})


app.listen(3660,()=>{
    console.log("Working....!");
})