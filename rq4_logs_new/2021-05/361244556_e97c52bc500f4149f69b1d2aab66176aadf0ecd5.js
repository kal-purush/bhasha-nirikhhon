const express=require('express')

const port=3001
host='localhost'

const app=express()
app.use(express.json())
app.use(express.static('public'));


app.get('/',(req,res)=>{
    res.send("use /ConfForm.html for the Conference registration form")
})

app.get('/ConfForm.html',(req,res)=>{
    res.sendFile( __dirname + "/" + "ConfForm.html" );
})

app.get('/register_info',  (req, res)=> {
    res.json({
        Full_Name: req.query['full_name'],
        City: req.query['City'],
        Branch: req.query['Branch'],
        Conference: req.query['Conference'],
    })
})


app.listen(port,host,()=>{
    console.log(`running on ${host} at port no ${port}`)
})