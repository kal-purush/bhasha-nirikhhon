const express = require('express')
const app = express()
const port = 5000


app.use(express.static('./public/dist'))
app.use(express.static('./node_modules/bootstrap/dist'))
app.use(express.static('./node_modules/@popperjs/core/dist/umd'))
app.set('view engine','ejs')


app.get('/',(req,res)=>{
    res.render('index.ejs')
})

app.get('/login',(req,res)=>{
    res.render('login.ejs')
})

app.get('/album',(req,res)=>{
    res.render('album.ejs')
})

app.listen(port,()=>{
    console.log(`The server started on port ${port}`)
})