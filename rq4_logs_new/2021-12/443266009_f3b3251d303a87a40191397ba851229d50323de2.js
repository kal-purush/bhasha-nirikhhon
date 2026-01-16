const express = require('express'); 
const path = require ('path'); 

const cors = require('cors');

const nav= [
    {
        link:"/books",
        title:"Books"
    },
    {
        link:"/authors",
        title:"Authors"
    },
    {
        link:"/books/addbook",
        title:"Add Book"
    },
    {
        link:"/authors/addauthor",
        title:"Add Author"
    }
]

const loginRouter = require('./src/routes/loginroute');
const signupRouter = require('./src/routes/signuproute');
const homeRoute = require('./src/routes/homerouter')(nav)//point 3 it was wrongly written as home route
const booksRouter = require('./src/routes/booksroute')(nav)//point 6
const authorsRouter = require('./src/routes/authorsroute')(nav)
const bodyParser = require('body-parser');

const app = express(); 


app.set('views','./src/views'); 
app.set('view engine','ejs');

app.use(cors());//point 7

//point 2 body parser declared now first it was not declared
app.use(bodyParser.urlencoded({extended:true}));
app.use(express.json());
app.use(express.static(path.join(__dirname , '/public'))); 

app.use('/login',loginRouter); 
app.use('/signup',signupRouter); 
app.use('/home',homeRoute); 
app.use('/books',booksRouter); 
app.use('/authors',authorsRouter); 



app.get('/',function(req,res){

    res.render('index',{nav});
    
});





app.listen(5000,()=>{
    console.log("Server Ready on 5000");// instead of 5000 it was written as 3000 point 5
});